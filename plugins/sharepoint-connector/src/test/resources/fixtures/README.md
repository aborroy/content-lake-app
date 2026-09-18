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

`<key>` is the file name, which is normally the item id. The one case where they differ is
`items/i-removed-deleted.json`, whose payload carries `"id": "i-removed"` and a `deleted` facet: two
different payloads for one item id cannot both be keyed by the id.

`children.txt` states the tree instead of deriving it from each item's `parentReference`, because deriving
it would mean parsing. Keep the two consistent by hand when adding a fixture.

## Replacing these with real payloads

These were written from Microsoft's documented shapes, not recorded from a tenant, so anything they get
wrong is wrong in the connector's tests too. Recording the real thing needs no app registration and no
Azure CLI: Graph Explorer at `https://developer.microsoft.com/graph/graph-explorer` runs the calls against
your own account in a browser and lets you copy the JSON out.

The five payloads worth replacing first, because each one is an assumption the ACL mapping depends on:

1. `permissions/root.json` and `permissions/i-quarterly.json`, for the exact claim strings behind
   "Everyone except external users" and for what `inheritedFrom` looks like on an inheriting item.
2. `permissions/i-group.json`, for whether `grantedToV2.group` really carries a usable Entra object id for
   both a security group and a Microsoft 365 group.
3. `permissions/i-named.json`, for whether a named grant carries `userPrincipalName` as well as an id.
4. `permissions/i-orgwide.json`, for what an organisation-scoped link's `link` facet actually contains.
5. Any `items/*.json`, for the fields a real `driveItem` carries that these omit.

## What the fixture set covers

| Fixture | Shape it exists for |
|---|---|
| `i-quarterly`, `i-incident` | inherited permissions; `i-incident` is markdown, which the pipeline short-circuits before any extractor |
| `i-named` | inheritance broken, granted to one named user, with `siteUser` alongside |
| `i-group` | granted to a security group only, which is fail-closed and retrievable by nobody until an Entra group resolver ships |
| `i-orgwide` | an organisation-scoped link, which maps to everyone, next to an anonymous link, which grants nothing |
| `i-deep` | two levels down, to prove traversal descends; also a `users`-scoped link with `grantedToIdentitiesV2` |
| `i-removed` | present in `order.txt` and deleted in `changes/1`, so a tombstone is reachable |
| `root` | a `siteGroup` grant, which no Entra resolver can expand and which therefore has to be counted and reported rather than silently dropped |
| `i-noacl` | an item whose permissions cannot be read, for the fail-closed path |

`i-noacl` is deliberately absent from `order.txt`, `children.txt` and `permissions/`, so a `/permissions`
call for it answers 404 while the item itself resolves. It is reachable only through `getNode`, which keeps
it out of the way of every test that counts what a walk or a delta pass returns.
