const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });
const propertyPrefix = 'hubspot__';
let expirationDate;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  const result = await hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken);

  const body = result.body ? result.body : result;

  const newAccessToken = body.accessToken;
  expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

  hubspotClient.setAccessToken(newAccessToken);

  if (newAccessToken !== accessToken) {
    account.accessToken = newAccessToken;
    await domain.save();
  }

  return true;
};

/**
 * Search HubSpot Contacts
 */
const searchContacts = async (domain, hubId, searchObject, tryCount = 1) => {
  try {
    return await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
  } catch (err) {
    if (tryCount >= 4) {
      throw new Error('Failed to fetch contacts for the 4th time. Aborting.');
    }

    if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

    await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));

    return await searchContacts(domain, hubId, searchObject, tryCount + 1);
  }
};

/**
 * Search HubSpot Companies
 */
const searchCompanies = async (domain, hubId, searchObject, tryCount = 1) => {
  try {
    return await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
  } catch (err) {
    if (tryCount >= 4) {
      throw new Error('Failed to fetch companies for the 4th time. Aborting.');
    }

    if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

    await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));

    return await searchCompanies(domain, hubId, searchObject, tryCount + 1);
  }
};

/**
 * Search HubSpot Meetings
 */
const searchMeetings = async (domain, hubId, searchObject, tryCount = 1) => {
  try {
    return await hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
  } catch (err) {
    if (tryCount >= 4) {
      throw new Error('Failed to fetch meetings for the 4th time. Aborting.');
    }

    if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

    await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));

    return await searchMeetings(domain, hubId, searchObject, tryCount + 1);
  }
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  const offsetObject = {
    lastModifiedDate: lastPulledDate,
    hasMore: true,
    after: 0
  };
  const limit = 100;

  while (offsetObject.hasMore) {
    const lastModifiedDateFilter = generateLastModifiedDateFilter(offsetObject.lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    const searchResult = await searchCompanies(domain, hubId, searchObject);
    const data = searchResult?.results ?? [];

    console.log('fetch company batch');

    if (!data?.length) {
      break;
    }

    offsetObject.hasMore = !!searchResult?.paging?.next?.after;
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    for (const company of data) {
      if (!company.properties) continue;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000, // Why subtract 2000 only for companies?
        ...actionTemplate
      });
    }

    if (offsetObject.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  const offsetObject = {
    lastModifiedDate: lastPulledDate,
    hasMore: true,
    after: 0
  };
  const limit = 100;

  while (offsetObject.hasMore) {
    const lastModifiedDateFilter = generateLastModifiedDateFilter(offsetObject.lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    const searchResult = await searchContacts(domain, hubId, searchObject);
    const data = searchResult?.results ?? [];

    console.log('fetch contact batch');

    if (!data?.length) {
      break;
    }

    offsetObject.hasMore = !!searchResult?.paging?.next?.after;
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    // contact to company association
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: data.map(contact => ({ id: contact.id })) }
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(
      a => [a?.from?.id, a.to[0].id]
    ).filter(([x]) => x));

    for (const contact of data) {
      if (!contact.properties || !contact.properties.email) continue;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    }

    if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified meetings as 100 contacts per page
 */
const processMeetings = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.meetings);
  const now = new Date();

  const contacts = new Map();

  const offsetObject = {
    lastModifiedDate: lastPulledDate,
    hasMore: true,
    after: 0
  };
  const limit = 100;

  while (offsetObject.hasMore) {
    const lastModifiedDateFilter = generateLastModifiedDateFilter(offsetObject.lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'hs_meeting_title',
        'hs_timestamp',
      ],
      limit,
      after: offsetObject.after
    };

    const searchResult = await searchMeetings(domain, hubId, searchObject);
    const data = searchResult?.results ?? [];

    console.log('fetch meetings batch');

    if (!data?.length) {
      break;
    }

    offsetObject.hasMore = !!searchResult?.paging?.next?.after;
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    const contactAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
      body: { inputs: data.map(meeting => ({ id: meeting.id })) }
    })).json())?.results || [];

    const contactIds = [...new Set(contactAssociationsResults.map(a => a.to[0].id))].filter(
      id => !contacts.has(id)
    );

    if (contactIds.length) {
      const contactsResult = await searchContacts(domain, hubId, {
        filterGroups: [
          {
            propertyName: 'id',
            operator: 'IN',
            values: contactIds
          }
        ],
        properties: [
          'email',
        ],
        limit: 100
      });

      for (const contact of contactsResult.results) {
        contacts.set(contact.id, contact);
      }
    }

    const contactAssociations = Object.fromEntries(contactAssociationsResults.map(
      a => [a?.from?.id, {
        id: a.to[0].id,
        email: contacts.get(a.to[0].id)?.properties?.email
      }]
    ).filter(([x]) => x));

    for (const meeting of data) {
      if (!meeting.properties || !meeting.properties.hs_meeting_title) continue;

      const contact = contactAssociations?.[meeting.id];

      const isCreated = new Date(meeting.createdAt) > lastPulledDate;

      const meetingProperties = {
        contact_id: contact?.id,
        contact_email: contact?.email,
        meeting_title: meeting.properties.hs_meeting_title,
        meeting_timestamp: meeting.properties.hs_timestamp,
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: meeting.id,
        meetingProperties: filterNullValuesFromObject(meetingProperties)
      };

      q.push({
        actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
        actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
        ...actionTemplate
      });
    }

    if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.meetings = now;
  await saveDomain(domain);

  return true;
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log('process mettings');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }
};

module.exports = pullDataFromHubspot;
