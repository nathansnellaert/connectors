"""CMS download step -- catalog connector over three CMS DCAT portals.

Chosen mechanism: ``bulk_csv``. Every collect entity is one dataset living on one of
three CMS data portals. We pull each dataset's full *current* table as structured rows,
one HTTP-paginated read per dataset:

  * ``main``     (data.cms.gov)               -> data-api/v1
        GET https://data.cms.gov/data-api/v1/dataset/{uuid}/data?size=&offset=
        Returns a JSON list of row objects. Paginate by offset until a short/empty page.
        (The bulk_csv distribution downloadURL is this same endpoint with format=csv +
        size=5000; we read JSON so we get clean per-row objects with no CSV parsing.)
  * ``medicaid`` (data.medicaid.gov)          -> DKAN datastore
        GET https://data.medicaid.gov/api/1/datastore/query/{id}/0?limit=500&offset=&count=true
        Returns {"results": [...], "count": N, ...}. Paginate by offset until count reached.
  * ``provider`` (data.cms.gov/provider-data) -> DKAN datastore, identical shape/root swap.

Observed contract (probed live):
  * DKAN caps ``limit`` at 500 (limit>=5000 -> HTTP 500); ``count`` is authoritative.
  * data.cms.gov data-api accepts large pages; we use 5000 (its own documented default).
  * All values arrive as strings, and the column set differs per dataset -> raw is written
    as NDJSON (zstd). One structured path that fits all three portals; transform re-types.

Refresh model: stateless full re-pull. Each dataset's current table is fetched in full and
overwritten every run. Research reports no incremental/`since` filter on these surfaces; the
only freshness signal is the DCAT ``modified`` timestamp, which the maintain step uses to gate
whether this fn runs. If invoked, we fetch -- no raw_asset_exists short-circuit here.

Node-id caveat: spec ids are ``cms-{entity_id.lower().replace('_','-')}``, which is lossy for
provider slug ids containing '_' (e.g. ``provider:clinical_depression``,
``provider:covid-19_hcp``). We keep the authoritative ids in ENTITY_IDS and recover the real
source id from node_id via _REAL_BY_NODE, so the portal id is reconstructed exactly.
"""
from __future__ import annotations

import httpx
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from subsets_utils import NodeSpec, get, save_raw_ndjson

ENTITY_IDS = [
    'main:01edb62e-5c45-4f43-8c91-16cba21cbb74',
    'main:029c119f-f79c-49be-9100-344d31d10344',
    'main:041d68a9-3212-42f3-89d5-b23e82103576',
    'main:04baec39-4a54-400e-824d-8e75251ceda9',
    'main:0764d86c-d19c-4b73-9e57-eba3cc1f7849',
    'main:086e48c4-87a6-4be1-8823-29e8da8f225b',
    'main:090bac79-c8b2-4e8a-bc3e-6bee002bcd6e',
    'main:09fd71b8-eb3e-45af-a01e-f8ab5a190e84',
    'main:0d753f51-c3de-43cd-95d2-550a23b8606a',
    'main:0d9eebff-7e23-4b1e-8e29-362eea132df5',
    'main:0e57f57d-0acc-4c9c-8f8c-973e3f4a3c4b',
    'main:1057bdab-3ef8-4057-86a6-88fdc973bd79',
    'main:113eb0bc-0c9a-4d91-9f93-3f6b28c0bf6b',
    'main:129a6503-c0f1-4132-b186-4c0232c2d894',
    'main:14d8e8a9-7e9b-4370-a044-bf97c46b4b44',
    'main:15f64ab4-3172-4a27-b589-ebd67a6d28aa',
    'main:164fc736-4179-4100-9f79-592b69e41975',
    'main:1746a83e-bb65-4300-8e02-21edbab77c6b',
    'main:175d576d-0568-4ac2-aec5-d77f3ee02205',
    'main:1cd9eded-d2c9-4215-a064-aac6dae3b714',
    'main:1e1beaba-9b41-47ca-960a-bd47b6ea65bd',
    'main:20f51cff-4137-4f3a-b6b7-bfc9ad57983b',
    'main:22c117c2-c04f-4078-9b7d-782167c8f0bb',
    'main:2457ea29-fc82-48b0-86ec-3b0755de7515',
    'main:24da2642-7269-4c75-9a62-0dc3a195b205',
    'main:24fe2a9a-4144-46b2-bf1a-07aa86fb65ae',
    'main:25704213-e833-4b8b-9dbc-58dd17149209',
    'main:261b83b6-b89f-43ad-ae7b-0d419a3bc24b',
    'main:2684c3e2-3598-4997-a598-0991bad6fbf2',
    'main:27c150fd-8578-43b1-bba5-6388987e32af',
    'main:2935c3fe-b18a-4e39-a0c5-e70573664f19',
    'main:2941ab09-8cee-49d8-9703-f3c5b854e388',
    'main:2cab9566-3495-4937-9925-3d9962ccf0f8',
    'main:31f25ab6-2fe3-4bad-ac5a-90635ed79935',
    'main:33be6c15-765c-424a-a2bf-a152d79dbd30',
    'main:3746498e-874d-45d8-9c69-68603cafea60',
    'main:3997fb87-a6d5-41d0-823f-7a62283e8035',
    'main:3b7e7659-067e-41ea-8e36-f9ee2036e1f6',
    'main:3ff3dcc3-7608-448d-9b35-4f184697e37c',
    'main:41d96997-415c-42fd-b473-81b7640a7ce2',
    'main:424de599-7a34-4243-af70-95d24dd675dd',
    'main:43ef03ce-2b60-40a8-958e-146195b5fec7',
    'main:44060663-47d8-4ced-a115-b53b4c270acb',
    'main:44e93e18-b9b3-4650-9471-2b1b31dc588b',
    'main:4999da74-1d8d-4a6f-934e-2d7ea470cc63',
    'main:4b50bbe6-a496-4eda-b03b-5f835937f81b',
    'main:4bae4223-a1dc-4b9c-bd7e-d9622461be35',
    'main:4bcae866-3411-439a-b762-90a6187c194b',
    'main:4c2a8bf6-8560-4b00-bc56-1a0322677b7f',
    'main:4ce4157f-4e02-4188-b43a-2b21b7769b4e',
    'main:4e73f1b5-82cb-4682-8ad2-28493f0b6840',
    'main:4ff7c618-4e40-483a-b390-c8a58c94fa15',
    'main:522f7e2d-58fc-4267-a073-8fa5fd18859f',
    'main:54551982-39a8-4744-90f6-c38bb4dd5108',
    'main:5ad4e138-a8b4-49e1-a2f4-34f52b3a665a',
    'main:5b227bd9-82d4-4145-86fd-809e02ca7f18',
    'main:5d40af0b-17a7-4f03-b0bb-bee1aa815d73',
    'main:5f2c306f-3b1c-42cd-b037-187b2ce22126',
    'main:5f9f1216-6fd9-455d-bfbc-0efade687a4e',
    'main:60625dc8-b621-45f0-9423-077fd133b13e',
    'main:619a72e4-07cc-414e-95d6-058e3c10557a',
    'main:6219697b-8f6c-4164-bed4-cd9317c58ebc',
    'main:62e490c0-9503-4b5f-9518-8e82fe20ccb6',
    'main:62e62d07-1837-4dbf-bb4f-a4820e0c7b16',
    'main:6395b458-2f89-4828-8c1a-e1e16b723d48',
    'main:63a83bb1-4c02-43b3-8ef4-e3d3c6cf62fa',
    'main:690ddc6c-2767-4618-b277-420ffb2bf27c',
    'main:69ec2609-5ce5-4ce1-b14c-1f8809fda2c2',
    'main:6a0dbf98-e4b0-4037-ac63-1439b08f4a71',
    'main:6a3aa708-3c9d-411a-a1a4-e046d3ade7ef',
    'main:6bd6b1dd-208c-4f9c-88b8-b15fec6db548',
    'main:6c3532b3-8325-48fd-a939-12b41d2b126a',
    'main:6c63099b-0794-40a0-925c-51a66b9b9901',
    'main:6d6fe0be-25d8-473b-84f6-b3b9ef3e4469',
    'main:6d7b229d-5bfb-4666-a2d2-38cea44a112c',
    'main:6fea9d79-0129-4e4c-b1b8-23cd86a4f435',
    'main:73b2ce14-351d-40ac-90ba-ec9e1f5ba80c',
    'main:75e8dcb2-78eb-4a7d-a377-9108441966db',
    'main:76a714ad-3a2c-43ac-b76d-9dadf8f7d890',
    'main:7adb8b1b-b85c-4ed3-b314-064776e50180',
    'main:7bd74bf4-e396-43a6-af12-42ffb31ee00b',
    'main:7cf9662e-7c5c-4fe0-a8c6-828edf81a23c',
    'main:7dae1d4a-1e14-4dd5-81b6-ba1dc0509a25',
    'main:7e0b4365-fd63-4a29-8f5e-e0ac9f66a81b',
    'main:7e0d53ba-8f02-4c66-98a5-14a1c997c50d',
    'main:7f93a63d-f2a2-4d29-b95b-c160185aaf90',
    'main:81d7cb4c-9e7e-43ab-8858-0a2250291935',
    'main:86b4807a-d63a-44be-bfdf-ffd398d5e623',
    'main:8708ca8b-8636-44ed-8303-724cbfaf78ad',
    'main:8889d81e-2ee7-448f-8713-f071038289b5',
    'main:8900b9c5-50b7-43de-9bdd-0d7113a8355e',
    'main:8ba0f9b4-9493-4aa0-9f82-44ea9468d1b5',
    'main:8e989bc0-2260-49a7-9c6d-8e9e10af7cea',
    'main:900059a0-0a10-4f2f-8c3d-d8da432a421e',
    'main:91ded8c4-7e64-42ff-a595-4a4eb55df910',
    'main:92396110-2aed-4d63-a6a2-5d6207d46a29',
    'main:939226be-b107-476e-8777-f199a840138a',
    'main:9400ca2c-b36a-4380-873d-380ea67a249d',
    'main:94d00f36-73ce-4520-9b3f-83cd3cded25c',
    'main:9552739e-3d05-4c1b-8eff-ecabf391e2e5',
    'main:9767cb68-8ea9-4f0b-8179-9431abc89f11',
    'main:97ecfad1-d3f1-4d42-b774-d74661d830bc',
    'main:9887a515-7552-4693-bf58-735c77af46d7',
    'main:9b0e7798-d945-48fc-9861-d38bb5083a74',
    'main:9d4218d7-bb48-4278-964b-01755f0d8a85',
    'main:a15c198e-4cf3-46ab-a30e-15c69bd13edd',
    'main:a2d56d3f-3531-4315-9d87-e29986516b41',
    'main:a3d35ba1-3ff4-48dd-91b4-8e1f9e7a19b7',
    'main:a4358712-e910-4eaf-8f24-5e90ba3cf8d0',
    'main:a6496a7d-4e19-479a-a9ad-d4c0a49e07c3',
    'main:a69d3df7-3f66-4a0d-b5b8-0d66049bd565',
    'main:a93f5362-2fe6-4b4d-8260-118be0d618e0',
    'main:a94b1015-1b93-476b-80ec-508b4169c8f5',
    'main:ab03c9bc-0c22-4ca4-b032-21dd3408210d',
    'main:ae8c9418-acc9-4442-b217-33291448f6b8',
    'main:afe44b85-cc6d-40d7-b5df-00ae8910d1d2',
    'main:b497431a-5b57-42c0-9016-90105b51841e',
    'main:b8135181-4274-4a11-a6cd-992090297ef5',
    'main:bb2a336c-0710-4de9-80ad-6a2a5cbdbdeb',
    'main:bbcffb70-4b07-4a0b-a783-0722b89315b5',
    'main:be64fce3-e835-4589-b46b-024198e524a6',
    'main:bf6a5b3b-31ee-4abb-b1ad-2607a1e7510a',
    'main:c04031db-54ce-461c-85d1-d2613d71f167',
    'main:c0451a3a-a86c-4bd4-a0b7-c93e6b1f1257',
    'main:c05cf65a-c13b-473c-8994-070f60f245b1',
    'main:c37ebe6d-f54f-4d7d-861f-fefe345554e6',
    'main:c3e8e9c3-5193-47fb-a5bb-d3ddb00e7197',
    'main:c8ea3f8e-3a09-4fea-86f2-8902fb4b0920',
    'main:c99b5865-1119-4436-bb80-c5af2773ea1f',
    'main:cab71ec9-221c-4969-93dc-196d9f824689',
    'main:cb2a224f-4d52-4cae-aa55-8c00c671384f',
    'main:cc3bd6db-2ae9-4d86-95be-20267f159f73',
    'main:ccbc9a44-40d4-46b4-a709-5caa59212e50',
    'main:d086edc0-4953-4fb9-a663-b35526371add',
    'main:d3eb38ac-d8e9-40d3-b7b7-6205d3d1dc16',
    'main:d65b8be0-946e-410b-ab06-01829628d5a1',
    'main:d7fabe1e-d19b-4333-9eff-e80e0643f2fd',
    'main:d8c69aca-2497-4a83-a181-706a960c77f0',
    'main:e0eba16f-ce0d-4037-96ce-2af70c718c98',
    'main:e1339c5d-8f24-46b0-95ac-32eb8d236f87',
    'main:e1f1fa9a-d6b4-417e-948a-c72dead8a41c',
    'main:e3db6e56-149f-49ce-b374-40aecda2357b',
    'main:e983965e-1603-4cb8-82b5-c40090e380d1',
    'main:eaed338b-847e-41b1-a4d3-a206f40dc72b',
    'main:ed289c89-0bb8-4221-a20a-85776066381b',
    'main:eddc0a1b-dab8-478e-a883-3385e0275a17',
    'main:ee6fb1a5-39b9-46b3-a980-a7284551a732',
    'main:f1a8c197-b53d-4c24-9770-aea5d5a97dfb',
    'main:f28a5c57-b4b2-4a3b-8c0e-18ab67c4d59b',
    'main:f557a6ed-95b3-4a22-8433-4175db2dec1c',
    'main:f6f6505c-e8b0-4d57-b258-e2b94133aaf2',
    'main:f7bc5d11-abce-4600-a680-a429f71e0653',
    'main:f8603e5b-9c47-4c52-9b47-a4ef92dfada4',
    'main:f8610e87-ba25-43a3-a49e-927dbc8701ae',
    'main:fc009b2d-7846-44b1-b4a1-692f0c143879',
    'medicaid:00505e90-f8ac-5921-b12f-5e23ba7ffcf3',
    'medicaid:0a2ec693-b322-54fb-a950-dfcdc873e3cf',
    'medicaid:0ad65fe5-3ad3-5d79-a3f9-7893ded7963a',
    'medicaid:0bef7b8a-c663-5b14-9a46-0b5c2b86b0fe',
    'medicaid:0d425780-16be-4ded-8420-69def8f4ee29',
    'medicaid:0e112ea8-8e8e-5dee-a7e2-7ed551c3baa4',
    'medicaid:0e203dba-396e-5f9f-a695-e702303dc713',
    'medicaid:138872f1-b1ba-5e05-be13-8be200306a58',
    'medicaid:158a1baa-5506-400a-8ec3-97756f0b0536',
    'medicaid:17169c68-671e-5fc3-ae52-73c1dc97aafb',
    'medicaid:17c4bf03-ba38-5c0a-9f08-96e9ed812bdf',
    'medicaid:1b03ec9b-07dd-4547-99a5-aacf206162d5',
    'medicaid:1c5d0fc9-693a-534a-8240-4627d9362b0d',
    'medicaid:1d3ffff8-2cf5-4cc9-a820-5879e957caa2',
    'medicaid:1db0f12c-e4d6-5a70-8c1b-5af9635999be',
    'medicaid:1fe73992-cbfd-5109-97bc-dee8b33fdcff',
    'medicaid:200c2cba-e58d-4a95-aa60-14b99736808d',
    'medicaid:229d6279-e614-5353-9226-f6a6f37d06c3',
    'medicaid:289182e5-44c8-4fcc-96c4-4af69be9863f',
    'medicaid:2b18f2f7-d0f3-5efe-afc4-4881fcbdf200',
    'medicaid:2b6a0ec0-efe6-5aec-9fe4-e168b8b6f553',
    'medicaid:2fed5758-5fd6-5dbb-8f92-34b3a0c3c8dd',
    'medicaid:4011579c-33ea-5741-a763-a2558d635561',
    'medicaid:45a28339-17a5-55e6-8e74-e9004fc703d8',
    'medicaid:46a5d780-feef-521a-af7b-25119ec3dc09',
    'medicaid:4723da0d-4d04-46ce-8163-e4b58c8fe728',
    'medicaid:47329369-e935-5de3-880f-d6a85a5fe9d1',
    'medicaid:4a00010a-132b-4e4d-a611-543c9521280f',
    'medicaid:4d4eaf55-33d3-4468-80b4-63553f4530ae',
    'medicaid:4d7af295-2132-55a8-b40c-d6630061f3e8',
    'medicaid:4d88c014-8989-5a88-bd21-17c1beed0dd3',
    'medicaid:50a46484-9a54-42d0-8da6-509a8b5656c0',
    'medicaid:52d7cf12-5c8d-450c-884d-7dca2632142a',
    'medicaid:52ed908b-0cb8-5dd2-846d-99d4af12b369',
    'medicaid:5394bcab-c748-5e4b-af07-b5bf77ed3aa3',
    'medicaid:53cf9f05-97e3-5bd6-a237-bc971e3642d9',
    'medicaid:5636a78c-fe18-4229-aee1-e40fa910a8a0',
    'medicaid:5670e72c-e44e-4282-ab67-4ebebaba3cbd',
    'medicaid:56adbcf4-f86b-5ab3-98fe-e713bbf99f12',
    'medicaid:57c03da7-5c35-54d5-a1d5-09600eea07c2',
    'medicaid:5abea2e0-3f8e-4b49-a50d-d63d5fd9103c',
    'medicaid:5b10a5b4-dabe-5419-99d8-043df47b8e4b',
    'medicaid:5b19d1d4-ae43-5fcd-ba14-3cecd99f473f',
    'medicaid:601a8897-1453-5282-81cd-be49d7ec7503',
    'medicaid:602e7c79-774d-529a-a377-75faa749e878',
    'medicaid:6165f45b-ca93-5bb5-9d06-db29c692a360',
    'medicaid:61729e5a-7aa8-448c-8903-ba3e0cd0ea3c',
    'medicaid:666bbb60-13ea-4545-b696-d65776db2bab',
    'medicaid:66da70e7-228e-41aa-b041-6f9e433ff237',
    'medicaid:69374797-d755-4903-bbe2-b84408501feb',
    'medicaid:6a91f80f-e2fd-5f43-8e38-afcebebb8387',
    'medicaid:6c114b2c-cb83-559b-832f-4d8b06d6c1b9',
    'medicaid:737f7186-b4d5-5272-a997-0ee94f4d4c3b',
    'medicaid:743f9f04-4473-41e2-9da2-9a89db65ee55',
    'medicaid:7656fc17-f1b4-566b-9a2d-c4a4f2ac7ae1',
    'medicaid:76a1984a-6d69-5e4d-86c8-65eb31f0506d',
    'medicaid:776a3880-a62d-5990-8b40-4406e6861dbb',
    'medicaid:79692ea5-21e1-56bf-8149-97d437120c4b',
    'medicaid:8062e2f4-4c0a-41c9-8217-979468a80986',
    'medicaid:80956a7d-e343-54f3-94a7-45d41b34fc0b',
    'medicaid:8185fa9f-cf59-49a4-9d67-10cef09f9aa4',
    'medicaid:8865d8e5-85fe-44f2-832d-2d6b984c312b',
    'medicaid:89de7fb8-b9ef-52fb-badc-ffc1128a9ada',
    'medicaid:8de1b213-73c5-552b-b84e-ac795f34d056',
    'medicaid:8e7be65b-97ba-5ecf-8394-ca8e6f63685a',
    'medicaid:8f39b637-9bb1-5894-9062-2c4f2ad70fba',
    'medicaid:915e5174-0869-5c6d-a5bb-454cb31ef605',
    'medicaid:91d4309d-3ca8-5a1e-8f78-79984027392d',
    'medicaid:927f4847-2c0a-50c1-8f50-9103de7d048b',
    'medicaid:992936b2-2a72-5df6-a734-a56a8631b87a',
    'medicaid:99315a95-37ac-4eee-946a-3c523b4c481e',
    'medicaid:9a83ba5e-05f5-47f5-82de-f3a59233a912',
    'medicaid:9e2d6cdf-a835-4b90-a5de-8a6c47eb471d',
    'medicaid:9fcb14ec-d5f0-536e-9938-3f0024531e5b',
    'medicaid:a058ef78-e18b-4435-94aa-b70ab6ce5904',
    'medicaid:a1f3598e-fc71-51aa-8560-78e7e1a61b09',
    'medicaid:a217613c-12bc-5137-8b3a-ada0e4dad1ff',
    'medicaid:a5023394-ab10-465b-bb4a-7de5ac98d90c',
    'medicaid:a54d7605-b780-4cf0-b53d-50313798f528',
    'medicaid:a6d2c261-210d-587c-8fa4-892444b7de42',
    'medicaid:a760aac0-5148-43e9-900a-bb128e667218',
    'medicaid:a9cfe5e9-d7d8-5b87-a7db-b45a7daf84fc',
    'medicaid:acbd1537-69b8-548d-a90f-eedb416acd71',
    'medicaid:ae4d5347-5137-5f6c-b66c-3420fa0316d8',
    'medicaid:b28a22a2-2c7b-5745-a3f9-d64621f51388',
    'medicaid:ba0c3734-8012-549a-8f50-2ff389d0e0ef',
    'medicaid:bb63ad8e-cab1-56e3-b492-da95d7e8cc5f',
    'medicaid:c1028fdf-2e43-5d5e-990b-51ed03428625',
    'medicaid:c39d3302-2d39-5209-9310-388ab1c8cbb8',
    'medicaid:c491f14b-6dd5-5e00-9e8d-0c49420e7caa',
    'medicaid:c5ac742e-cffa-4e04-a043-fb8324e5512c',
    'medicaid:c933dc16-7de9-52b6-8971-4b75992673e0',
    'medicaid:cc318bfb-a9b2-55f3-a924-d47376b32ea3',
    'medicaid:cdb769be-0d7b-524b-9d25-2062d36b60ab',
    'medicaid:ce4cf49b-a21b-5a53-bbc3-509414940847',
    'medicaid:d30cfc7c-4b32-4df1-b2bf-e0a850befd77',
    'medicaid:d32dc095-de60-45f0-9e58-852257970244',
    'medicaid:d371b0f4-db36-56f0-8aab-8f8ecb2b66e7',
    'medicaid:d5eaf378-dcef-5779-83de-acdd8347d68e',
    'medicaid:d7e4cccb-1c56-5b5d-acce-5e7744c6d3b4',
    'medicaid:d890d3a9-6b00-43fd-8b31-fcba4c8e2909',
    'medicaid:d8df4c15-38e0-43c8-ba3b-c56ffa61de5f',
    'medicaid:daba7980-e219-5996-9bec-90358fd156f1',
    'medicaid:dfa2ab14-06c2-457a-9e36-5cb6d80f8d93',
    'medicaid:dfd13757-d763-4f7a-9641-3f06ce21b4c6',
    'medicaid:e02d3bb0-600d-5451-945f-978a8a511770',
    'medicaid:e2ce0d2f-07c5-5213-947a-31e19bc649f6',
    'medicaid:e36d89c0-f62e-56d5-bc7e-b0adf89262b8',
    'medicaid:e3af839d-8175-5be0-b94e-4a302ed7a035',
    'medicaid:e6205a51-e6d7-4849-9882-4483b8a28c41',
    'medicaid:e85033c7-367e-467e-9e81-8e85048102b8',
    'medicaid:eba2c172-07ba-4a6a-877e-5373ca442243',
    'medicaid:ebcfc16f-8291-4c61-82a4-055846d72f3a',
    'medicaid:ec0d4b04-55bc-56d1-bff5-280f68915442',
    'medicaid:ee3b9534-0d19-4c1b-bf74-43f898d5de7c',
    'medicaid:eec7fbe6-c4c4-5915-b3d0-be5828ef4e9d',
    'medicaid:ef16c490-861a-4b1f-9e6d-f321abdcaab1',
    'medicaid:f3144e50-5f27-5e2c-acbb-1962a2aeae55',
    'medicaid:f38d0706-1239-442c-a3cc-40ef1b686ac0',
    'medicaid:f45f35c5-7aa4-4500-b196-ae7833717add',
    'medicaid:f7162680-b174-48b4-b3e3-3f0f735264ab',
    'medicaid:fbb83258-11c7-47f5-8b18-5f8e79f7e704',
    'medicaid:fbbe1734-b448-4e5a-bc94-3f8688534741',
    'medicaid:fc3c7c14-4b08-59c2-97db-0726e478dfdf',
    'provider:0127-af37',
    'provider:029b-dd7e',
    'provider:0330-b6e0',
    'provider:057a-5bcf',
    'provider:069d-826b',
    'provider:075a-d487',
    'provider:0938-3dfa',
    'provider:0b1a-da3f',
    'provider:0ba7-2cb0',
    'provider:0d7d-e988',
    'provider:0ddf-4325',
    'provider:0f37-cab0',
    'provider:0f6a-98c3',
    'provider:23ew-n7w9',
    'provider:2483-f62b',
    'provider:252m-zfp9',
    'provider:254e-694c',
    'provider:27ea-46a8',
    'provider:284v-j9fz',
    'provider:288b-4bed',
    'provider:2ca5-1007',
    'provider:2e55-8767',
    'provider:2fbd-6172',
    'provider:2fpu-cgbb',
    'provider:2rkq-ygai',
    'provider:32b2-9f88',
    'provider:32d8-3235',
    'provider:3614-1eef',
    'provider:385c-8c97',
    'provider:39da-b8ab',
    'provider:3a83-ee2d',
    'provider:3f64-129e',
    'provider:3n5g-6b7f',
    'provider:3xeb-u9wp',
    'provider:4269-8a74',
    'provider:4533-9861',
    'provider:46dc-a66c',
    'provider:48nr-hqxx',
    'provider:4a4e-e0da',
    'provider:4c74-462e',
    'provider:4gkm-5ypv',
    'provider:4j6d-yzce',
    'provider:4jcv-atw7',
    'provider:4pq5-n9py',
    'provider:501b-ecd4',
    'provider:56d7-4994',
    'provider:57e0-2991',
    'provider:59mq-zhts',
    'provider:5d65-6dcf',
    'provider:5d9c-1e86',
    'provider:5e3a-bee4',
    'provider:5f44-cb84',
    'provider:5gv4-jwyv',
    'provider:5hk7-b79m',
    'provider:5sqm-2qku',
    'provider:5zdx-ny2x',
    'provider:632h-zaca',
    'provider:6787-a1bf',
    'provider:6b8f-5372',
    'provider:6jpm-sxkc',
    'provider:6pfg-whmx',
    'provider:6qxe-iqz8',
    'provider:6uyb-waub',
    'provider:730a-fcd0',
    'provider:77hc-ibv8',
    'provider:7cv8-v37d',
    'provider:7d6a-e7a6',
    'provider:7peb-i4pi',
    'provider:7t8x-u3ir',
    'provider:8283-8a65',
    'provider:84jm-wiui',
    'provider:8634-f6a7',
    'provider:86e1-d1d1',
    'provider:8753-38d1',
    'provider:8c6b-fe40',
    'provider:8c70-d353',
    'provider:8c8a-b911',
    'provider:914a-7700',
    'provider:92bb-de79',
    'provider:94f7-ab53',
    'provider:95rg-2usp',
    'provider:9735-7176',
    'provider:97xg-v3wv',
    'provider:97z8-de96',
    'provider:9873-722a',
    'provider:98d4-0871',
    'provider:993f-4504',
    'provider:99ue-w85f',
    'provider:9b4a-75d5',
    'provider:9g7e-btyt',
    'provider:9n3s-kdb3',
    'provider:a0cb-cdab',
    'provider:a174-a962',
    'provider:a55e-5b88',
    'provider:adba-1d45',
    'provider:afcb-07d8',
    'provider:apyc-v239',
    'provider:avtz-f2ge',
    'provider:axe7-s95e',
    'provider:azum-44iv',
    'provider:b06f-0828',
    'provider:b554-ef7e',
    'provider:b599-54c1',
    'provider:b9bf-6883',
    'provider:bb4c-dcdf',
    'provider:bce0-b5db',
    'provider:bd7d-078a',
    'provider:bdd5-4a04',
    'provider:bs2r-24vh',
    'provider:bzsr-4my4',
    'provider:c14e-6492',
    'provider:c382-eab7',
    'provider:c44d-bde6',
    'provider:c713-00e8',
    'provider:c886-nwpj',
    'provider:c8a8-342e',
    'provider:ccbb-cbfa',
    'provider:ccn4-8vby',
    'provider:cd1d-5f84',
    'provider:cfa7-e909',
    'provider:clinical_depression',
    'provider:complete_qip_data',
    'provider:covid-19_hcp',
    'provider:ct36-nrcq',
    'provider:cvcs-xecj',
    'provider:d0ce-5cad',
    'provider:d150-d141',
    'provider:d1c9-d5b4',
    'provider:d226-80a4',
    'provider:d2k3-k3ac',
    'provider:d640-e528',
    'provider:d796-7f06',
    'provider:dc76-gh7x',
    'provider:dgck-syfz',
    'provider:dgmq-aat3',
    'provider:di9i-zzrc',
    'provider:djen-97ju',
    'provider:e1a1-c9b4',
    'provider:e2bb-d371',
    'provider:e491-a466',
    'provider:e84d-d357',
    'provider:ea2d-9467',
    'provider:ecb7-cb46',
    'provider:eda0-92f0',
    'provider:f226-42b7',
    'provider:f4ga-b9gx',
    'provider:f90c-2246',
    'provider:fa88-6ff2',
    'provider:fbfb-4b94',
    'provider:fche',
    'provider:footnotes',
    'provider:fp6g-2gsn',
    'provider:fykj-qjee',
    'provider:g6vv-u9sr',
    'provider:gxki-hrr8',
    'provider:hanv-ru8h',
    'provider:hbf-map',
    'provider:hicp-9999',
    'provider:hypercalcemia',
    'provider:if5v-4x48',
    'provider:ifjz-ge4w',
    'provider:ijh5-nb2v',
    'provider:isrn-hqyy',
    'provider:iy27-wz37',
    'provider:jfnd-nl7s',
    'provider:k2ze-bqvw',
    'provider:k653-4ka8',
    'provider:ka5z-ibe3',
    'provider:ktv_comprehensive',
    'provider:m5eg-upu5',
    'provider:m5jg-jg7i',
    'provider:medrec',
    'provider:mj5m-pzi6',
    'provider:muwa-iene',
    'provider:mxtu-43qs',
    'provider:n0yb-util',
    'provider:nasn-k89k',
    'provider:nhsn_bsi',
    'provider:nhsn_de',
    'provider:nrdb-3fcy',
    'provider:nrth-mfg3',
    'provider:pppw',
    'provider:ptds-r8im',
    'provider:pudb-wetr',
    'provider:q9vs-r7wp',
    'provider:qatj-nmws',
    'provider:qigt-w5cx',
    'provider:qip_ich_cahps',
    'provider:qmdc-9999',
    'provider:qoeg-w7ck',
    'provider:qqw3-t4ie',
    'provider:r5ix-sfxw',
    'provider:rrqw-56er',
    'provider:rs6n-9qwg',
    'provider:s5pj-hua3',
    'provider:s5xg-sys6',
    'provider:shr',
    'provider:srr',
    'provider:strr',
    'provider:su9h-3pvj',
    'provider:svdt-c123',
    'provider:tagd-9999',
    'provider:tbry-pc2d',
    'provider:tee5-ixt5',
    'provider:tf3h-mrrs',
    'provider:tps',
    'provider:tqkv-mgxq',
    'provider:u625-zae7',
    'provider:ujcx-uaut',
    'provider:uk3n-au7a',
    'provider:utgq-v46w',
    'provider:uyx4-5s7f',
    'provider:v9e4-nwhh',
    'provider:vat_topic',
    'provider:vtqa-m4zn',
    'provider:vxub-6swi',
    'provider:wkfw-kthe',
    'provider:wue8-3vwe',
    'provider:x663-bwbj',
    'provider:xcdc-v8bm',
    'provider:xrgf-x36b',
    'provider:xubh-q36u',
    'provider:y2hd-n93e',
    'provider:y9us-9xdf',
    'provider:yc9t-dgbk',
    'provider:yd3s-jyhd',
    'provider:yizn-abxn',
    'provider:ynj2-r877',
    'provider:ypbt-wvdk',
    'provider:yq43-i98g',
    'provider:yv7e-xc69',
    'provider:z8ax-x9j1',
    'provider:zez1-ka2w',
    'provider:zt3q-3e1z',
]

# node_id -> authoritative entity id (reverses the lossy lower/hyphenate so provider slugs
# with underscores resolve to the exact DKAN dataset id).
_REAL_BY_NODE = {f"cms-{e.lower().replace('_', '-')}": e for e in ENTITY_IDS}

_DKAN_ROOT = {
    "medicaid": "https://data.medicaid.gov",
    "provider": "https://data.cms.gov/provider-data",
}
_MAIN_DATA = "https://data.cms.gov/data-api/v1/dataset/{ident}/data"

MAIN_PAGE_SIZE = 5000       # data.cms.gov data-api default page; accepts larger but 5000 is safe.
DKAN_PAGE_SIZE = 500        # DKAN hard cap -- limit>=5000 returns HTTP 500.
_MAX_PAGES = 50000          # safety ceiling; raises so unexpected source growth surfaces loudly.
_LOG_EVERY = 20             # progress log cadence (pages).

_TRANSIENT_EXC = (
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
    httpx.WriteTimeout, httpx.PoolTimeout, httpx.RemoteProtocolError, httpx.ProxyError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_EXC):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


@retry(
    retry=retry_if_exception(_is_transient),
    stop=stop_after_attempt(6),
    wait=wait_exponential(min=4, max=120),
    reraise=True,
)
@sleep_and_retry
@limits(calls=5, period=1)   # ~5 rps/process -- polite (research: ~5-10 rps observed safe, no documented limit)
def _get_json(url: str, params: dict):
    resp = get(url, params=params, timeout=(10.0, 300.0))
    resp.raise_for_status()
    return resp.json()


def _iter_main(ident: str):
    """Yield every row of a data.cms.gov data-api dataset via offset pagination."""
    offset = 0
    pages = 0
    while True:
        rows = _get_json(_MAIN_DATA.format(ident=ident), {"size": MAIN_PAGE_SIZE, "offset": offset})
        if not isinstance(rows, list):
            raise TypeError(f"main {ident}: expected list page, got {type(rows).__name__}")
        if not rows:
            break
        for row in rows:
            yield row
        pages += 1
        offset += len(rows)
        if pages % _LOG_EVERY == 0:
            print(f"[cms main {ident}] {pages} pages, {offset} rows", flush=True)
        if len(rows) < MAIN_PAGE_SIZE:
            break
        if pages > _MAX_PAGES:
            raise RuntimeError(f"main {ident}: exceeded {_MAX_PAGES} pages -- source larger than expected")


def _iter_dkan(root: str, ident: str):
    """Yield every row of a DKAN datastore (medicaid / provider-data) via offset pagination."""
    offset = 0
    pages = 0
    total = None
    while True:
        params = {"limit": DKAN_PAGE_SIZE, "offset": offset}
        if total is None:
            params["count"] = "true"
        payload = _get_json(f"{root}/api/1/datastore/query/{ident}/0", params)
        if not isinstance(payload, dict):
            raise TypeError(f"dkan {ident}: expected dict payload, got {type(payload).__name__}")
        if total is None:
            total = int(payload.get("count") or 0)
        results = payload.get("results") or []
        if not results:
            break
        for row in results:
            yield row
        pages += 1
        offset += len(results)
        if pages % _LOG_EVERY == 0:
            print(f"[cms dkan {ident}] {pages} pages, {offset}/{total} rows", flush=True)
        if offset >= total or len(results) < DKAN_PAGE_SIZE:
            break
        if pages > _MAX_PAGES:
            raise RuntimeError(f"dkan {ident}: exceeded {_MAX_PAGES} pages -- source larger than expected")


def _logged(rows, label: str):
    n = 0
    for row in rows:
        n += 1
        yield row
    print(f"[cms {label}] done: {n} rows", flush=True)


def fetch_one(node_id: str) -> None:
    """Fetch one CMS dataset's full current table and write it as NDJSON (zstd).

    The runtime passes the spec id, which is also the asset name. The authoritative source
    id (portal + dataset id) is recovered from _REAL_BY_NODE. Stateless full re-pull; the
    generator streams page-by-page into save_raw_ndjson so memory stays bounded to one page.
    """
    asset = node_id
    entity_id = _REAL_BY_NODE.get(node_id)
    if entity_id is None:
        raise KeyError(f"unknown node_id {node_id!r} (not in ENTITY_IDS)")
    portal, ident = entity_id.split(":", 1)

    if portal == "main":
        rows = _iter_main(ident)
    elif portal in _DKAN_ROOT:
        rows = _iter_dkan(_DKAN_ROOT[portal], ident)
    else:
        raise ValueError(f"unknown portal {portal!r} for entity {entity_id!r}")

    save_raw_ndjson(_logged(rows, entity_id), asset)


DOWNLOAD_SPECS = [
    NodeSpec(id=f"cms-{eid.lower().replace('_', '-')}", fn=fetch_one, kind="download")
    for eid in ENTITY_IDS
]
