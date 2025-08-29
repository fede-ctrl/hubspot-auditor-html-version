const express = require('express');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());

// --- ENV Vars ---
const CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;
const RENDER_EXTERNAL_URL = process.env.RENDER_EXTERNAL_URL;
const REDIRECT_URI = `${RENDER_EXTERNAL_URL}/api/oauth-callback`;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// --- HubSpot API Helpers ---
async function getNewAccessToken(refreshToken) {
    const response = await fetch('https://api.hubapi.com/oauth/v1/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
            grant_type: 'refresh_token',
            client_id: CLIENT_ID,
            client_secret: CLIENT_SECRET,
            refresh_token: refreshToken,
        }),
    });
    if (!response.ok) throw new Error('Failed to refresh access token');
    return await response.json();
}

async function getValidAccessToken(portalId) {
    const { data: installation, error } = await supabase
        .from('installations')
        .select('refresh_token, access_token, expires_at')
        .eq('hubspot_portal_id', portalId)
        .single();

    if (error || !installation) throw new Error(`Could not find installation for portal ${portalId}. Please reinstall the app.`);

    let { refresh_token, access_token, expires_at } = installation;

    if (new Date() > new Date(expires_at)) {
        const newTokens = await getNewAccessToken(refresh_token);
        access_token = newTokens.access_token;
        const newExpiresAt = new Date(Date.now() + newTokens.expires_in * 1000).toISOString();
        await supabase.from('installations').update({ access_token, expires_at: newExpiresAt }).eq('hubspot_portal_id', portalId);
    }
    return access_token;
}

// --- Routes ---
app.get('/', (req, res) => res.status(200).send('HubSpot Auditor server is live and healthy.'));

app.get('/api/install', (req, res) => {
  const SCOPES = 'oauth crm.objects.companies.read crm.objects.contacts.read crm.schemas.companies.read crm.schemas.contacts.read';
  const authUrl = `https://app.hubspot.com/oauth/authorize?client_id=${CLIENT_ID}&redirect_uri=${REDIRECT_URI}&scope=${SCOPES}`;
  res.redirect(authUrl);
});

app.get('/api/oauth-callback', async (req, res) => {
  const authCode = req.query.code;
  if (!authCode) return res.status(400).send('HubSpot authorization code not found.');
  try {
    const response = await fetch('https://api.hubapi.com/oauth/v1/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'authorization_code', client_id: CLIENT_ID, client_secret: CLIENT_SECRET, redirect_uri: REDIRECT_URI, code: authCode }),
    });
    if (!response.ok) throw new Error(await response.text());
    const tokenData = await response.json();
    const { refresh_token, access_token, expires_in } = tokenData;
    const tokenInfoResponse = await fetch(`https://api.hubapi.com/oauth/v1/access-tokens/${access_token}`);
    if (!tokenInfoResponse.ok) throw new Error('Failed to fetch HubSpot token info');
    const tokenInfo = await tokenInfoResponse.json();
    const hub_id = tokenInfo.hub_id;
    const expiresAt = new Date(Date.now() + expires_in * 1000).toISOString();
    await supabase.from('installations').upsert({ hubspot_portal_id: hub_id, refresh_token, access_token, expires_at: expiresAt }, { onConflict: 'hubspot_portal_id' });
    res.status(200).send(`<h1>Success!</h1><p>Your connection has been saved. You can now close this page.</p>`);
  } catch (error) {
    console.error(error);
    res.status(500).send(`<h1>Server Error</h1><p>${error.message}</p>`);
  }
});

// --- REBUILT AUDIT ROUTE (STABLE & FAST) ---
app.get('/api/audit', async (req, res) => {
  const portalId = req.header('X-HubSpot-Portal-Id');
  const objectType = req.query.objectType || 'contacts';
  if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });

  try {
    const accessToken = await getValidAccessToken(portalId);
    
    // 1. Get all non-archived properties for the object to know what to look for
    const propertiesUrl = `https://api.hubapi.com/crm/v3/properties/${objectType}?archived=false`;
    const propertiesResponse = await fetch(propertiesUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });
    if (!propertiesResponse.ok) throw new Error(`Failed to fetch properties for ${objectType}`);
    const propertiesData = await propertiesResponse.json();
    // Filter out HubSpot's internal 'hs_' properties for a cleaner audit
    const allProperties = propertiesData.results.filter(p => !p.name.startsWith('hs_'));
    const propertyNames = allProperties.map(p => p.name);
    
    // 2. Get the EXACT total number of records
    const searchUrl = `https://api.hubapi.com/crm/v3/objects/${objectType}/search`;
    const totalCountResponse = await fetch(searchUrl, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ limit: 1, properties: ["hs_object_id"] }),
    });
    if (!totalCountResponse.ok) throw new Error('Failed to fetch total record count');
    const totalCountData = await totalCountResponse.json();
    const totalRecords = totalCountData.total;

    // 3. Fetch a large sample of records (up to 1000) with all their properties
    let recordsSample = [];
    let after = undefined;
    const MAX_PAGES = 10; // Fetch up to 10 pages (10 * 100 = 1000 records)
    for (let i = 0; i < MAX_PAGES; i++) {
        const sampleUrl = `https://api.hubapi.com/crm/v3/objects/${objectType}?limit=100&properties=${propertyNames.join(',')}` + (after ? `&after=${after}` : '');
        const sampleResponse = await fetch(sampleUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });
        if (!sampleResponse.ok) break;
        const sampleData = await sampleResponse.json();
        recordsSample.push(...sampleData.results);
        if (sampleData.paging && sampleData.paging.next) {
            after = sampleData.paging.next.after;
        } else {
            break; // No more pages
        }
    }
    
    // Return early if no records are found to avoid division by zero
    if (recordsSample.length === 0) {
        return res.json({
            totalRecords: 0,
            totalProperties: allProperties.length,
            propertiesWithZeroFillRate: allProperties.length,
            averageFillRate: 0,
            properties: allProperties.map(p => ({ label: p.label, internalName: p.name, type: p.type, fillRate: 0, fillCount: 0 })),
        });
    }

    // 4. Calculate fill counts by analyzing the sample LOCALLY
    const fillCounts = {};
    for (const record of recordsSample) {
        for (const propName in record.properties) {
            const value = record.properties[propName];
            if (value !== null && value !== '') {
                fillCounts[propName] = (fillCounts[propName] || 0) + 1;
            }
        }
    }

    // 5. Build the final results
    const auditResults = allProperties.map(prop => {
        const fillCountInSample = fillCounts[prop.name] || 0;
        // Extrapolate the fill count from the sample to the total record count
        const estimatedTotalFillCount = Math.round((fillCountInSample / recordsSample.length) * totalRecords);
        const fillRate = totalRecords > 0 ? Math.round((estimatedTotalFillCount / totalRecords) * 100) : 0;
        
        return {
            label: prop.label,
            internalName: prop.name,
            type: prop.type,
            fillRate,
            fillCount: estimatedTotalFillCount,
        };
    });

    // 6. Calculate summary stats
    const propertiesWithZeroFillRate = auditResults.filter(p => p.fillRate === 0).length;
    const averageFillRate = auditResults.length > 0
        ? Math.round(auditResults.reduce((acc, p) => acc + p.fillRate, 0) / auditResults.length)
        : 0;

    res.json({
        totalRecords: totalRecords,
        totalProperties: auditResults.length,
        propertiesWithZeroFillRate: propertiesWithZeroFillRate,
        averageFillRate: averageFillRate,
        properties: auditResults,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: error.message });
  }
});


app.listen(PORT, () => console.log(`✅ Server is live on port ${PORT}`));

