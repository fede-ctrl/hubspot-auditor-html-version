require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const CLIENT_ID = process.env.HUBSPOT_CLIENT_ID;
const CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET;
const RENDER_EXTERNAL_URL = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
const REDIRECT_URI = `${RENDER_EXTERNAL_URL}/api/oauth-callback`;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

async function getValidAccessToken(portalId) {
    const { data: installation, error } = await supabase.from('installations').select('refresh_token, access_token, expires_at').eq('hubspot_portal_id', portalId).single();
    if (error || !installation) throw new Error(`Could not find installation for portal ${portalId}. Please reinstall the app.`);
    let { refresh_token, access_token, expires_at } = installation;
    if (new Date() > new Date(expires_at)) {
        const response = await fetch('https://api.hubapi.com/oauth/v1/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ grant_type: 'refresh_token', client_id: CLIENT_ID, client_secret: CLIENT_SECRET, refresh_token }),
        });
        if (!response.ok) throw new Error('Failed to refresh access token');
        const newTokens = await response.json();
        access_token = newTokens.access_token;
        const newExpiresAt = new Date(Date.now() + newTokens.expires_in * 1000).toISOString();
        await supabase.from('installations').update({ access_token, expires_at: newExpiresAt }).eq('hubspot_portal_id', portalId);
    }
    return access_token;
}

app.get('/api/install', (req, res) => {
    const SCOPES = 'oauth crm.objects.companies.read crm.objects.contacts.read crm.schemas.companies.read crm.schemas.contacts.read reports_read automation';
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
        res.send(`<h1>Success!</h1><p>Your connection has been saved. You can now close this tab and return to the application.</p>`);
    } catch (error) {
        console.error(error);
        res.status(500).send(`<h1>Server Error</h1><p>${error.message}</p>`);
    }
});

app.get('/api/audit', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    const objectType = req.query.objectType || 'contacts';
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const accessToken = await getValidAccessToken(portalId);
        const propertiesUrl = `https://api.hubapi.com/crm/v3/properties/${objectType}?archived=false`;
        const propertiesResponse = await fetch(propertiesUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });
        if (!propertiesResponse.ok) throw new Error(`Failed to fetch properties for ${objectType}`);
        const propertiesData = await propertiesResponse.json();
        const allProperties = propertiesData.results;
        const propertyNames = allProperties.map(p => p.name);

        const totalCountResponse = await fetch(`https://api.hubapi.com/crm/v3/objects/${objectType}/search`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ limit: 1, properties: ["hs_object_id"] }),
        });
        if (!totalCountResponse.ok) throw new Error('Failed to fetch total record count');
        const totalCountData = await totalCountResponse.json();
        const totalRecords = totalCountData.total;

        let recordsSample = [];
        let after = undefined;
        for (let i = 0; i < 10; i++) {
            const sampleUrl = `https://api.hubapi.com/crm/v3/objects/${objectType}?limit=100&properties=${propertyNames.join(',')}` + (after ? `&after=${after}` : '');
            const sampleResponse = await fetch(sampleUrl, { headers: { 'Authorization': `Bearer ${accessToken}` } });
            if (!sampleResponse.ok) break;
            const sampleData = await sampleResponse.json();
            recordsSample.push(...sampleData.results);
            if (sampleData.paging && sampleData.paging.next) { after = sampleData.paging.next.after; } else { break; }
        }

        if (recordsSample.length === 0) {
            return res.json({
                totalRecords: 0, totalProperties: allProperties.length, averageCustomFillRate: 0, propertiesWithZeroFillRate: allProperties.length,
                properties: allProperties.map(p => ({ label: p.label, internalName: p.name, type: p.type, description: p.description || '', isCustom: !p.hubspotDefined, fillRate: 0, fillCount: 0 })),
            });
        }

        const fillCounts = {};
        recordsSample.forEach(r => Object.keys(r.properties).forEach(p => { if (r.properties[p] !== null && r.properties[p] !== '') fillCounts[p] = (fillCounts[p] || 0) + 1; }));

        const auditResults = allProperties.map(prop => {
            const fillCountInSample = fillCounts[prop.name] || 0;
            const estimatedTotalFillCount = Math.round((fillCountInSample / recordsSample.length) * totalRecords);
            const fillRate = totalRecords > 0 ? Math.round((estimatedTotalFillCount / totalRecords) * 100) : 0;
            return { label: prop.label, internalName: prop.name, type: prop.type, description: prop.description || '', isCustom: !prop.hubspotDefined, fillRate, fillCount: estimatedTotalFillCount };
        });

        const customProperties = auditResults.filter(p => p.isCustom);
        const averageCustomFillRate = customProperties.length > 0 ? Math.round(customProperties.reduce((acc, p) => acc + p.fillRate, 0) / customProperties.length) : 0;
        const propertiesWithZeroFillRate = auditResults.filter(p => p.fillRate === 0).length;

        res.json({ totalRecords, totalProperties: auditResults.length, averageCustomFillRate, propertiesWithZeroFillRate, properties: auditResults });
    } catch (error) {
        console.error(`Audit error for ${objectType}:`, error);
        res.status(500).json({ message: error.message });
    }
});

app.get('/api/data-health', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const accessToken = await getValidAccessToken(portalId);
        
        const orphanedContactsSearch = { filterGroups: [{ filters: [{ propertyName: 'associatedcompanyid', operator: 'NOT_HAS_PROPERTY' }] }], limit: 1 };
        const orphanedContactsRes = await fetch('https://api.hubapi.com/crm/v3/objects/contacts/search', { method: 'POST', headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' }, body: JSON.stringify(orphanedContactsSearch) });
        const orphanedContactsData = await orphanedContactsRes.json();

        const emptyCompaniesSearch = { filterGroups: [{ filters: [{ propertyName: 'num_associated_contacts', operator: 'EQ', value: 0 }] }], limit: 1 };
        const emptyCompaniesRes = await fetch('https://api.hubapi.com/crm/v3/objects/companies/search', { method: 'POST', headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' }, body: JSON.stringify(emptyCompaniesSearch) });
        const emptyCompaniesData = await emptyCompaniesRes.json();
        
        const contactSampleRes = await fetch(`https://api.hubapi.com/crm/v3/objects/contacts?limit=100&properties=email`, { headers: { 'Authorization': `Bearer ${accessToken}` }});
        const contactSampleData = await contactSampleRes.json();
        const emailCounts = contactSampleData.results.reduce((acc, c) => { const email = c.properties.email?.toLowerCase(); if(email) acc[email] = (acc[email] || 0) + 1; return acc; }, {});
        const contactDuplicatesInSample = Object.values(emailCounts).filter(c => c > 1).length;
        
        const companySampleRes = await fetch(`https://api.hubapi.com/crm/v3/objects/companies?limit=100&properties=domain`, { headers: { 'Authorization': `Bearer ${accessToken}` }});
        const companySampleData = await companySampleRes.json();
        const domainCounts = companySampleData.results.reduce((acc, c) => { const domain = c.properties.domain?.toLowerCase(); if(domain) acc[domain] = (acc[domain] || 0) + 1; return acc; }, {});
        const companyDuplicatesInSample = Object.values(domainCounts).filter(c => c > 1).length;

        res.json({
            orphanedContacts: orphanedContactsData.total || 0,
            emptyCompanies: emptyCompaniesData.total || 0,
            contactDuplicatesInSample,
            companyDuplicatesInSample
        });
    } catch (error) {
        console.error("Data Health Audit Error:", error);
        res.status(500).json({ message: error.message });
    }
});

app.get('/api/stale-reports', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const accessToken = await getValidAccessToken(portalId);
        const allReports = [];
        let after = null;
        do {
            const url = `https://api.hubapi.com/reports/v3/reports` + (after ? `?after=${after}` : '');
            const response = await fetch(url, { headers: { 'Authorization': `Bearer ${accessToken}` } });
            if (!response.ok) {
                 throw new Error('Failed to fetch reports. Your HubSpot account may not have access to this API or the required permissions were not granted.');
            }
            const data = await response.json();
            allReports.push(...data.results);
            after = data.paging?.next?.after || null;
        } while (after);
        const staleThreshold = new Date();
        staleThreshold.setDate(staleThreshold.getDate() - 180);
        const staleReports = allReports.filter(report => new Date(report.updatedAt) < staleThreshold)
            .map(report => ({ name: report.name, id: report.id, updatedAt: report.updatedAt.split('T')[0] }));
        staleReports.sort((a, b) => new Date(a.updatedAt) - new Date(b.updatedAt));
        res.json({ staleReports });
    } catch (error) {
        console.error("Stale Reports Audit Error:", error);
        res.status(500).json({ message: error.message });
    }
});

app.get('/api/inactive-workflows', async (req, res) => {
    const portalId = req.header('X-HubSpot-Portal-Id');
    if (!portalId) return res.status(400).json({ message: 'HubSpot Portal ID is missing.' });
    try {
        const accessToken = await getValidAccessToken(portalId);
        const allWorkflows = [];
        let after = null;
        do {
            const url = `https://api.hubapi.com/automation/v3/workflows` + (after ? `?after=${after}` : '');
            const response = await fetch(url, { headers: { 'Authorization': `Bearer ${accessToken}` } });
            if (!response.ok) {
                 throw new Error('Failed to fetch workflows. Your HubSpot account may not have access to this API or the required permissions were not granted.');
            }
            const data = await response.json();
            allWorkflows.push(...data.results);
            after = data.paging?.next?.after || null;
        } while (after);
        const inactiveWorkflows = allWorkflows.filter(wf => wf.enabled === false)
            .map(wf => ({ name: wf.name, id: wf.id, updatedAt: wf.updatedAt.split('T')[0] }));
        inactiveWorkflows.sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));
        res.json({ inactiveWorkflows });
    } catch (error) {
        console.error("Inactive Workflows Audit Error:", error);
        res.status(500).json({ message: error.message });
    }
});

app.listen(PORT, () => console.log(`✅ Server is live on port ${PORT}`));