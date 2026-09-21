# Mock Graph fixtures

Payloads served by `MockGraphServer`. Every file is returned to the connector verbatim: the mock parses no
JSON, so anything recorded from a real tenant can be dropped in unchanged. That is the point of the layout.

## Layout

| Path | Serves |
|---|---|
| `site.json` | `GET /v1.0/sites/{siteId}` |
| `drives.json` | `GET /v1.0/sites/{siteId}/drives` |
| `drive.json` | `GET /v1.0/drives/{driveId}` |
| `items/<key>.json` | `GET /v1.0/drives/{driveId}/items/{itemId}` |
| `permissions/<key>.json` | `GET /v1.0/drives/{driveId}/items/{itemId}/permissions` |
| `content/<key>` | the bytes behind `GET .../items/{itemId}/content` |
| `order.txt` | the order the delta feed reports items in |
| `children.txt` | the tree, as `<parentId>: <childId> <childId> ...` |
| `changes/<n>/order.txt` | what the n-th incremental delta call reports |
| `users/<identity>/groups.json` | `GET /v1.0/users/{identity}/transitiveMemberOf/microsoft.graph.group` |

`<key>` is the file name, which is normally the item id. The one case where they differ is
`items/i-removed-deleted.json`, whose payload carries `"id": "i-removed"` and a `deleted` facet: two
different payloads for one item id cannot both be keyed by the id.

`children.txt` states the tree instead of deriving it from each item's `parentReference`, because deriving
it would mean parsing. Keep the two consistent by hand when adding a fixture.

## Two invariants a new fixture has to keep

**An item carries the `shared` facet if and only if it has permissions of its own.** Under
`Prefer: hierarchicalsharing` that facet is the only thing distinguishing a permission-hierarchy root from an
item that inherits, so `PermissionHierarchyCache` reads the facet and nothing else. A fixture whose
`permissions/` payload has no `inheritedFrom` and whose `items/` payload has no `shared` facet would be
served an ancestor's ACL in `hierarchical` mode: that is an over-share, and it would be the fixture's fault
rather than the connector's.

**An inheriting item's `permissions/` payload lists every entry it inherits**, which means every entry of
the nearest ancestor that has its own. Graph returns the full effective collection for an item, not the
difference from its parent, and `SharePointConnectorClientTest` asserts that both permission modes grant an
inheriting document to the same principals. A payload listing only some of what it inherits breaks that
without being wrong about anything the connector can see in `per-item` mode.

The mock serves each `items/` payload verbatim in both modes, so it cannot reproduce what Graph returns when
the preference is *not* honoured, where `shared` appears on inheriting items too. It does not need to: the
connector refuses to run in `hierarchical` mode against a tenant that does not echo `Preference-Applied`, so
it never reads the facet in that case.

## Replacing these with real payloads

These were written from Microsoft's documented shapes, not recorded from a tenant, so anything they get
wrong is wrong in the connector's tests too, and the tests would agree with the mock while both were wrong
about the same thing. That is `aborroy/content-lake-app#142`. What follows is the runbook for closing it.

It needs **no app registration**, which matters because that is what is blocked: the tenant used for
development has "Register applications" set to No, and the Azure CLI's borrowed Graph token carries no files
or sites scope. Graph Explorer signs in as you and needs only consent for `Files.Read`.

### Use your own OneDrive, not a SharePoint site

This is an API constraint rather than a preference. Graph returns an item's permissions **by caller**: to the
item's owner it returns every sharing permission, and to a non-owner it returns *only the permissions that
apply to that caller* (documented on `driveItem: list permissions`). So a site you are merely a member of
hands back a truncated ACL, and a fixture built from it looks entirely plausible and is wrong in the one
direction that matters, because it encodes a narrower ACL than reality.

Your own OneDrive for Business has the same `driveItem` API, the same `delta` and the same sharing model, and
you own every item, so the full permission set comes back.

### Step 0: build the tree and the shares

The payloads only exist if the sharing does, so this is the actual work. In your own OneDrive create:

| Create | Then share it as | It becomes |
|---|---|---|
| folder `cl-public` with two files in it | share the **folder** with one colleague, view only | the inheritance case: the folder has its own ACL, both files inherit it |
| file `cl-named.txt` in `cl-public` | share the **file** with a different colleague, view only | inheritance broken to a named user |
| file `cl-group.txt` | share with a **security group or a Microsoft 365 group**, view only | the group case, which is the one the Entra resolver depends on |
| file `cl-orgwide.txt` | "Copy link", audience **People in your organisation**, view only | an organisation-scoped link |
| file `cl-anon.txt` | "Copy link", audience **Anyone**, if your tenant allows it | an anonymous link, which must grant nothing |
| folder `cl-nested/cl-level-two/cl-deep.txt` | leave unshared | proves traversal, and shows what an item with no sharing of its own looks like |

Note the group case may need someone who can create a group. If that is not available, record what you can
and say so: a missing group fixture is better than an invented one.

### Step 1: the requests, in order

Graph Explorer has a **Request headers** tab, which steps 4 and 5 need. Run each of these and keep the whole
response body, not an excerpt.

| # | Request | Answers | Replaces |
|---|---|---|---|
| 1 | `GET /me/drive` | the drive's own shape, and gives you the drive id | `drive.json` |
| 2 | `GET /me/drive/root` | what a drive root carries, including whether `parentReference` has an `id` at all | `items/root.json` |
| 3 | `GET /me/drive/root/children` | the fields a real `driveItem` carries that these fixtures omit | every `items/*.json` |
| 4 | `GET /me/drive/root/delta?$top=2` | the paged delta shape: `@odata.nextLink` then `@odata.deltaLink` | nothing directly; the mock builds pages by concatenating item files |
| 5 | `GET /me/drive/root/delta` **with header `Prefer: hierarchicalsharing`** | **the most important one**; see below | nothing directly |
| 6 | `GET /me/drive/items/{id}/permissions` for each item in step 0 | every grant shape | `permissions/*.json` |
| 7 | Delete one shared file, then re-run the `deltaLink` from step 4 | what a `deleted` facet looks like | `items/i-removed-deleted.json` |

### Step 2: what to look for, question by question

Three of `#142`'s five questions still change code. The other two became a setting and a refusal, so they are
worth confirming and no longer block.

**Q3, the `shared` facet, is now the one that matters most.** Since the permission-hierarchy cache
(`#140`), the connector decides "this item inherits" from the *absence* of the `shared` facet, and that is
only meaningful when Graph honoured `Prefer: hierarchicalsharing`. In step 5, check:

- the **`Preference-Applied` response header**. If `hierarchicalsharing` is not in it, the preference was not
  honoured, which is itself an answer: it would mean the delegated `Files.Read` route cannot exercise this at
  all and the question stays open for app-only with `Sites.FullControl.All`.
- if it *was* honoured, whether `shared` appears on the two files inside `cl-public`. It must **not**: they
  inherit. It must appear on `cl-public` itself, on `cl-named.txt` and on the individually shared files. If
  an inheriting file carries `shared`, `PermissionHierarchyCache` is reading a signal that does not mean what
  it thinks, and that is a correctness bug rather than a fixture update.

**Q2, what `grantedToV2` contains** for each identity. For every entry in step 6, check whether a usable
Entra object id is present:

- `grantedToV2.user` should carry both `id` and `userPrincipalName`. The mapper emits both.
- `grantedToV2.group` **must** carry an `id`. The mapper refuses to emit a group by display name, because
  Entra display names are not unique, so a shape with no id means that grant is silently dropped and the
  document under-shares. If a Microsoft 365 group differs from a security group here, both shapes are needed.
- whether `siteUser`/`siteGroup` appear *alongside* the directory form. The mapper assumes they routinely do
  and prefers the directory one.

**Q5, what `roles` holds** for a plain "can view" grant. `read`, `write` and `owner` grant read; anything
else grants nothing and is counted. A tenant emitting some other token means those documents are retrievable
by nobody, silently.

**Q1, the everyone claim strings**, is worth checking but may not be answerable here: "Everyone except
external users" is a SharePoint site principal, and an organisation-scoped OneDrive link is likely to appear
as `link.scope = "organization"` instead. Either way it is now the `sharepoint.everyone-claims` setting plus a
substring match on the structured `spo-grid-all-users` fragment, so a difference is configuration.

### Step 3: land them

Paste each response in verbatim. The mock parses no JSON, so no reshaping is needed and none should be done:
that is what makes a recorded payload usable. Rename the ids in `order.txt` and `children.txt` to match, and
keep the two invariants in the section above, since the hierarchy tests depend on them:

- an item carries `shared` if and only if its `permissions` payload has an entry without `inheritedFrom`;
- an inheriting item's `permissions` payload lists **everything** it inherits, not the difference from its
  parent. Ten fixtures were originally wrong about this and it read as the cache over-sharing.

Then `mvn -f plugins/sharepoint-connector/pom.xml test` and, on a running stack,
`USE_HTTPS=false ./test/test-sharepoint.sh` from the deployment repository. Real payloads failing those tests
is the whole point of the exercise: it means an assumption was wrong, and the fixture is right.

### What this route cannot answer

State these on the issue rather than leaving them implied:

- **Site and `siteGroup` principals**, `GET /sites` enumeration, and app-only authentication. A site-local
  principal is the case no Entra resolver can ever expand, so its shape stays unconfirmed.
- **A `410 Gone` resync.** It needs a genuinely aged-out delta token and cannot be produced on demand.
- **Real throttling**, and therefore `#142`'s "measured resource-unit cost per operation" criterion: Graph
  does not return a resource-unit figure on a SharePoint response, so the per-operation prices remain
  Microsoft's published table. What *is* measured, from the connector's own meter, is the cost per document:
  5.60 units per item against 1.10 hierarchical.

## What the fixture set covers

| Fixture | Shape it exists for |
|---|---|
| `i-quarterly`, `i-incident` | inherited permissions; `i-incident` is markdown, which the pipeline short-circuits before any extractor |
| `i-named` | inheritance broken, granted to one named user, with `siteUser` alongside |
| `i-group` | granted to a security group only, so it retrieves for a member of that group and for nobody else |
| `i-orgwide` | an organisation-scoped link, which maps to everyone, next to an anonymous link, which grants nothing |
| `i-deep` | two levels down, to prove traversal descends; also a `users`-scoped link with `grantedToIdentitiesV2` |
| `i-removed` | present in `order.txt` and deleted in `changes/1`, so a tombstone is reachable |
| `root` | a `siteGroup` grant, which no Entra resolver can expand and which therefore has to be counted and reported rather than silently dropped |
| `i-noacl` | an item whose permissions cannot be read, for the fail-closed path |
| `i-report` | a PDF, the only fixture whose text an extractor has to recover |
| `i-paged` | permissions spread over two pages, so the collection pager is exercised |
| `f-public` and its four children | one hierarchy root's ACL inherited by five items, which is what makes "a folder of N files costs one permission call, not N" measurable |
| `users/sp-member@contoso.com` | in Finance, so this user retrieves the group-granted document |
| `users/sp-outsider@contoso.com` | not in Finance, so this user must not |

`i-noacl` is deliberately absent from `order.txt`, `children.txt` and `permissions/`, so a `/permissions`
call for it answers 404 while the item itself resolves. It is reachable only through `getNode`, which keeps
it out of the way of every test that counts what a walk or a delta pass returns.

## Group membership, for the query path

`users/<identity>/groups.json` serves the transitive membership the RAG service's Entra resolver reads, so
one fixture set answers both halves of an ACL claim: the directory that grants `group-grant.txt` to Finance is
the same directory that says who is in Finance. A missing fixture is a 404, which the resolver reads as "no
such identity" rather than as "in no groups"; the two are different answers and collapsing them either
blacks out a caller or quietly downgrades a fail-closed deployment.
