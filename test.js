const hubspot = require('@hubspot/api-client');

const hubspotClient = new hubspot.Client({ accessToken: "pat-na1-5854142f-fd52-44d5-abf5-44604c2f4669" });
console.log("AUTH token", hubspotClient)
async function main() {
    // Search for contacts
    const searchRequest = {
        filterGroups: [
            {
                filters: [
                    {
                        propertyName: 'cs_account_id',
                        operator: 'IN',
                        values: ["184248", "184237"]
                    }
                ]
            }
        ],
        properties: ["cs_account_id"]
    };

    const response = await hubspotClient.crm.contacts.searchApi.doSearch(searchRequest);
    // const data = await response.json();
    console.log(response);
}

(async () => {
    await main();
})();

2026-02-03T17:32:05.180Z	INFO	Beginning association preparation process
2026-02-03T17:32:05.231Z	INFO	[START] Preparing associations for event ID: 16385334040 (CS: 4726)
2026-02-03T17:32:05.231Z	INFO	[INFO] Found 0 previously failed account IDs
2026-02-03T17:32:05.231Z	INFO	[INFO] Fetching registrations for event 4726
2026-02-03T17:32:07.011Z	INFO	[INFO] Total registrations fetched: 165
2026-02-03T17:32:07.012Z	INFO	[INFO] Finding HubSpot contacts for 165 account IDs
2026-02-03T17:32:07.012Z	INFO	[INFO] Processing batch 1, size=100
2026-02-03T17:32:08.890Z	INFO	[INFO] Batch fetched 103 contacts
2026-02-03T17:32:08.890Z	INFO	[INFO] Processing batch 2, size=65
2026-02-03T17:32:09.191Z	INFO	[INFO] Batch fetched 66 contacts
2026-02-03T17:32:09.191Z	INFO	[INFO] Found 165 unique cs_account_id values in HubSpot
2026-02-03T17:32:09.192Z	INFO	[UJ]  Not account ids:  []
2026-02-03T17:32:09.193Z	INFO	[INFO] Event end date: 2025-07-25T00:00:00.000Z
2026-02-03T17:32:09.193Z	INFO	[INFO] Event has ended
2026-02-03T17:32:09.193Z	INFO	[INFO] Processing: 0 registered, 165 attended, 0 no-show
2026-02-03T17:32:09.230Z	INFO	[SUCCESS] Association preparation completed: 165 contacts to process, 0 failed account IDs
2026-02-03T17:32:09.230Z	INFO	[INFO] Moving to association batch processing
2026-02-03T17:32:09.230Z	INFO	Queue status: 07_ASC_BATCH_A
2026-02-03T17:32:09.231Z	INFO	[ERROR_MANAGER] No new errors — skipping save
2026-02-03T17:32:09.231Z	INFO	[INFO] Moving to association batch processing

Memory: 84/128 MB
Runtime: 4096.50 ms


#############

2026-02-03T17:49:50.280Z	INFO	Beginning association preparation process
2026-02-03T17:49:50.280Z	INFO	[START] Preparing associations for event ID: 16385334040 (CS: 4726)
2026-02-03T17:49:50.281Z	INFO	[INFO] Found 0 previously failed account IDs
2026-02-03T17:49:50.281Z	INFO	[INFO] Fetching registrations for event 4726
2026-02-03T17:49:51.063Z	INFO	[INFO] Total registrations fetched: 165
2026-02-03T17:49:51.063Z	INFO	[INFO] Finding HubSpot contacts for 165 account IDs
2026-02-03T17:49:51.063Z	INFO	[INFO] Processing batch 1, size=100
2026-02-03T17:49:51.682Z	INFO	[INFO] Batch fetched 206 contacts
2026-02-03T17:49:51.702Z	INFO	[INFO] Processing batch 2, size=65
2026-02-03T17:49:51.923Z	INFO	[INFO] Batch fetched 66 contacts
2026-02-03T17:49:51.923Z	INFO	[INFO] Found 165 unique cs_account_id values in HubSpot
2026-02-03T17:49:51.923Z	INFO	[UJ]  Not account ids:  []
2026-02-03T17:49:51.923Z	INFO	[INFO] Event end date: 2025-07-25T00:00:00.000Z
2026-02-03T17:49:51.923Z	INFO	[INFO] Event has ended
2026-02-03T17:49:51.923Z	INFO	[INFO] Processing: 0 registered, 165 attended, 0 no-show
2026-02-03T17:49:51.923Z	INFO	[SUCCESS] Association preparation completed: 165 contacts to process, 0 failed account IDs
2026-02-03T17:49:51.923Z	INFO	[INFO] Moving to association batch processing
2026-02-03T17:49:51.923Z	INFO	Queue status: 07_ASC_BATCH_A
2026-02-03T17:49:51.924Z	INFO	[ERROR_MANAGER] No new errors — skipping save
2026-02-03T17:49:51.924Z	INFO	[INFO] Moving to association batch processing

Memory: 87/128 MB
Runtime: 1763.11 ms



async function findHubSpotContactIds(client, accountIds) {
    console.log(`[INFO] Finding HubSpot contacts for ${accountIds.length} account IDs`);

    // accountId -> Set(contactIds)
    const contactMap = new Map();
     165
    for (let i = 0; i < accountIds.length; i += 100) { 100/165
        const batchIds = accountIds
            .slice(i, i + 100)
            .map(id => String(id).trim());

        console.log(`[INFO] Processing batch ${i / 100 + 1}, size=${batchIds.length}`);

        let after = undefined;
        let totalFetched = 0;

        try {
            do {
                const response = await client.crm.contacts.searchApi.doSearch({
                    filterGroups: [{
                        filters: [{
                            propertyName: 'cs_account_id',
                            operator: 'IN',
                            values: batchIds
                        }]
                    }],
                    properties: ['cs_account_id', 'email', 'firstname', 'lastname'],
                    limit: 100,
                    after,
                    archived: false
                });

                totalFetched += response.results.length;

                response.results.forEach(contact => {
                    const accountId = contact.properties?.cs_account_id?.trim();
                    if (!accountId) return;

                    if (!contactMap.has(accountId)) {
                        contactMap.set(accountId, new Set());
                    }
                    contactMap.get(accountId).add(contact.id);
                });

                after = response.paging?.next?.after;
            } while (after);

            console.log(`[INFO] Batch fetched ${totalFetched} contacts`);
        } catch (error) {
            console.error('[ERROR] Contact search failed:', error.message);
        }
    }

    console.log(`[INFO] Found ${contactMap.size} unique cs_account_id values in HubSpot`);
    return contactMap;
}


2026-02-03T18:02:27.331Z	INFO	Beginning association preparation process
2026-02-03T18:02:27.356Z	INFO	[START] Preparing associations for event ID: 16385334040 (CS: 4726)
2026-02-03T18:02:27.356Z	INFO	[INFO] Found 0 previously failed account IDs
2026-02-03T18:02:27.356Z	INFO	[INFO] Fetching registrations for event 4726
2026-02-03T18:02:28.954Z	INFO	[INFO] Total registrations fetched: 165
2026-02-03T18:02:28.954Z	INFO	[UJ] ALLACCOUNTIDS  [
  '151764', '184248', '184237', '177587', '176121', '180979',
  '181180', '181316', '180821', '180695', '180876', '180978',
  '181212', '181358', '182023', '182034', '181173', '181308',
  '181238', '180822', '180699', '180732', '180757', '180849',
  '180933', '165876', '170896', '170898', '180976', '181000',
  '181047', '181060', '181066', '173134', '179258', '179298',
  '179299', '179319', '179352', '179405', '179734', '180384',
  '180419', '180385', '180425', '158889', '180577', '180623',
  '166292', '180674', '180677', '173667', '173840', '178800',
  '163712', '178899', '163897', '164279', '176582', '176785',
  '173135', '179061', '163926', '179063', '169453', '179191',
  '179194', '160721', '160748', '178145', '178178', '165955',
  '160093', '178599', '178611', '178621', '178647', '178670',
  '160402', '178240', '178257', '178129', '178280', '178287',
  '177911', '177697', '159379', '177848', '177860', '177862',
  '177495', '161841', '178034', '177351', '159285', '159283',
  '161434', '177542', '161436', '176853',
  ... 65 more items
]
2026-02-03T18:02:28.993Z	INFO	[INFO] Finding HubSpot contacts for 165 account IDs
2026-02-03T18:02:28.993Z	INFO	[INFO] Processing batch 1, size=100
2026-02-03T18:02:30.833Z	INFO	[INFO] Batch fetched 206 contacts
2026-02-03T18:02:30.833Z	INFO	[INFO] Processing batch 2, size=65
2026-02-03T18:02:31.093Z	INFO	[INFO] Batch fetched 66 contacts
2026-02-03T18:02:31.093Z	INFO	[INFO] Found 165 unique cs_account_id values in HubSpot
2026-02-03T18:02:31.094Z	INFO	[UJ]  Not account ids:  []
2026-02-03T18:02:31.094Z	INFO	[INFO] Event end date: 2025-07-25T00:00:00.000Z
2026-02-03T18:02:31.094Z	INFO	[INFO] Event has ended
2026-02-03T18:02:31.094Z	INFO	[INFO] Processing: 0 registered, 165 attended, 0 no-show
2026-02-03T18:02:31.094Z	INFO	[SUCCESS] Association preparation completed: 165 contacts to process, 0 failed account IDs
2026-02-03T18:02:31.094Z	INFO	[INFO] Moving to association batch processing
2026-02-03T18:02:31.094Z	INFO	Queue status: 07_ASC_BATCH_A
2026-02-03T18:02:31.095Z	INFO	[ERROR_MANAGER] No new errors — skipping save
2026-02-03T18:02:31.095Z	INFO	[INFO] Moving to association batch processing

Memory: 86/128 MB
Runtime: 3803.98 ms

########

WARNING: The logs for this function have exceeded the 4KB limit.
...
2026-02-03T18:07:08.437Z	INFO	Beginning association preparation process
START RequestId: c40f1361-2eed-414e-b8bd-15cdd6b4f345 Version: $LATEST
2026-02-03T18:07:08.438Z	INFO	[START] Preparing associations for event ID: 16385334040 (CS: 4726)
2026-02-03T18:07:08.438Z	INFO	[INFO] Found 0 previously failed account IDs
2026-02-03T18:07:08.438Z	INFO	[INFO] Fetching registrations for event 4726
2026-02-03T18:07:09.184Z	INFO	[INFO] Total registrations fetched: 165
2026-02-03T18:07:09.184Z	INFO	[UJ] ALLACCOUNTIDS  165
2026-02-03T18:07:09.185Z	INFO	[INFO] Finding HubSpot contacts for 165 account IDs
2026-02-03T18:07:09.185Z	INFO	[INFO] Processing batch 1, size=100
2026-02-03T18:07:10.943Z	INFO	[INFO] Batch fetched 206 contacts
2026-02-03T18:07:10.943Z	INFO	[INFO] Processing batch 2, size=65
2026-02-03T18:07:11.424Z	INFO	[INFO] Batch fetched 66 contacts
2026-02-03T18:07:11.424Z	INFO	[INFO] Found 165 unique cs_account_id values in HubSpot
2026-02-03T18:07:11.424Z	INFO	[UJ]  Not account ids:  []
2026-02-03T18:07:11.424Z	INFO	[INFO] Event end date: 2025-07-25T00:00:00.000Z
2026-02-03T18:07:11.424Z	INFO	[INFO] Event has ended
2026-02-03T18:07:11.424Z	INFO	[INFO] Processing: 0 registered, 165 attended, 0 no-show
2026-02-03T18:07:11.425Z	INFO	[SUCCESS] Association preparation completed: 165 contacts to process, 0 failed account IDs
2026-02-03T18:07:11.425Z	INFO	[INFO] Moving to association batch processing
2026-02-03T18:07:11.425Z	INFO	Queue status: 07_ASC_BATCH_A
2026-02-03T18:07:11.425Z	INFO	[ERROR_MANAGER] No new errors — skipping save
2026-02-03T18:07:11.425Z	INFO	[INFO] Moving to association batch processing

Memory: 88/128 MB
Runtime: 3347.46 ms



2026-02-03T18:17:21.930Z	INFO	Beginning association preparation process
2026-02-03T18:17:21.973Z	INFO	[START] Preparing associations for event ID: 16385334040 (CS: 4726)
2026-02-03T18:17:21.973Z	INFO	[INFO] Found 0 previously failed account IDs
2026-02-03T18:17:21.973Z	INFO	[INFO] Fetching registrations for event 4726
2026-02-03T18:17:23.674Z	INFO	[INFO] Total registrations fetched: 165
2026-02-03T18:17:23.693Z	INFO	[UJ] ALLACCOUNTIDS  165
2026-02-03T18:17:23.694Z	INFO	[INFO] Finding HubSpot contacts for 165 account IDs
2026-02-03T18:17:25.315Z	INFO	[UJ] search response length  100
2026-02-03T18:17:25.316Z	INFO	[UJ] total search response length  undefined
2026-02-03T18:17:25.594Z	INFO	[UJ] search response length  66
2026-02-03T18:17:25.594Z	INFO	[UJ] total search response length  undefined
2026-02-03T18:17:25.594Z	INFO	[INFO] Found 163 matching contacts in HubSpot
2026-02-03T18:17:25.594Z	INFO	[UJ]  Not account ids:  [ '184248', '184237' ]
2026-02-03T18:17:25.596Z	INFO	[WARN] 2 account IDs not found in HubSpot
2026-02-03T18:17:25.653Z	INFO	[INFO] Event end date: 2025-07-25T00:00:00.000Z
2026-02-03T18:17:25.653Z	INFO	[INFO] Event has ended
2026-02-03T18:17:25.653Z	INFO	[INFO] Skipped: 2 due to previous failures, 0 due to missing contacts
2026-02-03T18:17:25.653Z	INFO	[INFO] Processing: 0 registered, 163 attended, 0 no-show
2026-02-03T18:17:25.654Z	INFO	[SUCCESS] Association preparation completed: 163 contacts to process, 2 failed account IDs
2026-02-03T18:17:25.654Z	INFO	[INFO] Moving to association batch processing
2026-02-03T18:17:25.654Z	INFO	Queue status: 07_ASC_BATCH_A
2026-02-03T18:17:25.654Z	INFO	[ERROR_MANAGER] Processing 1 new error type(s)
2026-02-03T18:17:25.654Z	INFO	[ERROR_MANAGER] Previous errors exist — starting merge process
2026-02-03T18:17:25.654Z	INFO	[ERROR_MANAGER] Loaded previous errors: 2 type(s)
2026-02-03T18:17:25.654Z	INFO	[ERROR_MANAGER] Existing: 17 errors, 0 unique emails, 0 root validation
2026-02-03T18:17:25.654Z	INFO	[ERROR_MANAGER] Merge result: +2 added, -0 duplicate, -0 downstream
2026-02-03T18:17:25.856Z	INFO	[ERROR_MANAGER] ✓ Saved 19 total errors
2026-02-03T18:17:25.857Z	INFO	[INFO] Moving to association batch processing

Memory: 87/128 MB
Runtime: 3965.21 ms


WARNING: The logs for this function have exceeded the 4KB limit.
...
9', '176582', '176785',
  '173135', '179061', '163926', '179063', '169453', '179191',
  '179194', '160721', '160748', '178145', '178178', '165955',
  '160093', '178599', '178611', '178621', '178647', '178670',
  '160402', '178240', '178257', '178129', '178280', '178287',
  '177911', '177697', '159379', '177848', '177860', '177862',
  '177495', '161841', '178034', '177351', '159285', '159283',
  '161434', '177542', '161436', '176853'
]
2026-02-04T09:47:12.138Z	INFO	[UJ] search response length  100
2026-02-04T09:47:12.139Z	INFO	[UJ] total search response length  103
2026-02-04T09:47:12.139Z	INFO	batchid-> [
  '175102', '175109', '177042', '176998', '176804',
  '176886', '176981', '176999', '177007', '177016',
  '176578', '175200', '163659', '176639', '176640',
  '163596', '170299', '176741', '176791', '176528',
  '176524', '176521', '175537', '175337', '175527',
  '175530', '173754', '163425', '161900', '163484',
  '163360', '163359', '163371', '163438', '175671',
  '164755', '176445', '176454', '169689', '176473',
  '176474', '174198', '158311', '158312', '175237',
  '159159', '174352', '174092', '174286', '172988',
  '173592', '172861', '173162', '172863', '172847',
  '163127', '158794', '172697', '168494', '172291',
  '172387', '172398', '172406', '167461', '152205'
]
2026-02-04T09:47:12.358Z	INFO	[UJ] search response length  66
2026-02-04T09:47:12.358Z	INFO	[UJ] total search response length  66
2026-02-04T09:47:12.358Z	INFO	[INFO] Found 163 matching contacts in HubSpot
2026-02-04T09:47:12.358Z	INFO	[UJ]  Not account ids:  [ '184248', '184237' ]
2026-02-04T09:47:12.358Z	INFO	[WARN] 2 account IDs not found in HubSpot
2026-02-04T09:47:12.358Z	INFO	[INFO] Event end date: 2025-07-25T00:00:00.000Z
2026-02-04T09:47:12.358Z	INFO	[INFO] Event has ended
2026-02-04T09:47:12.358Z	INFO	[INFO] Skipped: 2 due to previous failures, 0 due to missing contacts
2026-02-04T09:47:12.359Z	INFO	[INFO] Processing: 0 registered, 163 attended, 0 no-show
2026-02-04T09:47:12.359Z	INFO	[SUCCESS] Association preparation completed: 163 contacts to process, 2 failed account IDs
2026-02-04T09:47:12.359Z	INFO	[INFO] Moving to association batch processing
2026-02-04T09:47:12.359Z	INFO	Queue status: 07_ASC_BATCH_A
2026-02-04T09:47:12.359Z	INFO	[ERROR_MANAGER] Processing 1 new error type(s)
2026-02-04T09:47:12.359Z	INFO	[ERROR_MANAGER] Previous errors exist — starting merge process
2026-02-04T09:47:12.359Z	INFO	[ERROR_MANAGER] Loaded previous errors: 2 type(s)
2026-02-04T09:47:12.359Z	INFO	[ERROR_MANAGER] Existing: 28 errors, 2 unique emails, 0 root validation
2026-02-04T09:47:12.359Z	INFO	[MERGE] Skip duplicate: braxtonhaff33@gmail.com (MISSING_DATA)
2026-02-04T09:47:12.359Z	INFO	[MERGE] Skip duplicate: pdunn@actsacademycolorado.com (MISSING_DATA)
2026-02-04T09:47:12.359Z	INFO	[ERROR_MANAGER] Merge result: +0 added, -2 duplicate, -0 downstream
2026-02-04T09:47:12.524Z	INFO	[ERROR_MANAGER] ✓ Saved 28 total errors
2026-02-04T09:47:12.524Z	INFO	[INFO] Moving to association batch processing

Memory: 90/128 MB
Runtime: 2111.04 ms




WARNING: The logs for this function have exceeded the 4KB limit.
...
176981',
      email: 'csharpe@haywoodchristianacademy.org',
      firstname: 'Cindy',
      hs_object_id: '107045411868',
      lastmodifieddate: '2026-02-04T05:26:33.470Z',
      lastname: 'Sharpe'
    },
    updatedAt: 2026-02-04T05:26:33.470Z
  },
  SimplePublicObject {
    createdAt: 2025-03-18T22:30:21.398Z,
    archived: false,
    id: '107045411869',
    properties: {
      createdate: '2025-03-18T22:30:21.398Z',
      cs_account_id: '177007',
      email: 'gboyd@haywoodchristianacademy.org',
      firstname: 'Gina',
      hs_object_id: '107045411869',
      lastmodifieddate: '2025-12-19T16:10:14.392Z',
      lastname: 'Boyd'
    },
    updatedAt: 2025-12-19T16:10:14.392Z
  },
  SimplePublicObject {
    createdAt: 2025-05-21T04:54:02.235Z,
    archived: false,
    id: '123176760428',
    properties: {
      createdate: '2025-05-21T04:54:02.235Z',
      cs_account_id: '172697',
      email: 'bmassey@palmyrachristianacademy.com',
      firstname: 'Beth',
      hs_object_id: '123176760428',
      lastmodifieddate: '2026-01-02T05:18:58.362Z',
      lastname: 'Massey'
    },
    updatedAt: 2026-01-02T05:18:58.362Z
  }
]
2026-02-04T10:05:18.364Z	INFO	[UJ] search response length  66
2026-02-04T10:05:18.364Z	INFO	[UJ] total search response length  66
2026-02-04T10:05:18.365Z	INFO	[DUP] 164755 [
  {
    email: 'dahrens@faithbaptistdavison.com',
    hs_object_id: '84621051'
  },
  { email: 'dahrens@fbcdavison.com', hs_object_id: '70169509432' }
]
2026-02-04T10:05:18.365Z	INFO	[INFO] Found 163 matching contacts in HubSpot
2026-02-04T10:05:18.365Z	INFO	[UJ]  Not account ids:  [ '184248', '184237' ]
2026-02-04T10:05:18.365Z	INFO	[WARN] 2 account IDs not found in HubSpot
2026-02-04T10:05:18.366Z	INFO	[INFO] Event end date: 2025-07-25T00:00:00.000Z
2026-02-04T10:05:18.366Z	INFO	[INFO] Event has ended
2026-02-04T10:05:18.423Z	INFO	[INFO] Skipped: 2 due to previous failures, 0 due to missing contacts
2026-02-04T10:05:18.423Z	INFO	[INFO] Processing: 0 registered, 163 attended, 0 no-show
2026-02-04T10:05:18.423Z	INFO	[SUCCESS] Association preparation completed: 163 contacts to process, 2 failed account IDs
2026-02-04T10:05:18.423Z	INFO	[INFO] Moving to association batch processing
2026-02-04T10:05:18.423Z	INFO	Queue status: 07_ASC_BATCH_A
2026-02-04T10:05:18.424Z	INFO	[ERROR_MANAGER] Processing 1 new error type(s)
2026-02-04T10:05:18.424Z	INFO	[ERROR_MANAGER] Previous errors exist — starting merge process
2026-02-04T10:05:18.424Z	INFO	[ERROR_MANAGER] Loaded previous errors: 2 type(s)
2026-02-04T10:05:18.424Z	INFO	[ERROR_MANAGER] Existing: 36 errors, 2 unique emails, 0 root validation
2026-02-04T10:05:18.424Z	INFO	[MERGE] Skip duplicate: braxtonhaff33@gmail.com (MISSING_DATA)
2026-02-04T10:05:18.424Z	INFO	[MERGE] Skip duplicate: pdunn@actsacademycolorado.com (MISSING_DATA)
2026-02-04T10:05:18.424Z	INFO	[ERROR_MANAGER] Merge result: +0 added, -2 duplicate, -0 downstream
2026-02-04T10:05:18.725Z	INFO	[ERROR_MANAGER] ✓ Saved 36 total errors
2026-02-04T10:05:18.725Z	INFO	[INFO] Moving to association batch processing

Memory: 90/128 MB
Runtime: 3950.77 ms


