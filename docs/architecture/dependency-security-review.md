# Documentation dependency security review

Reviewed on 2026-10-09.

You can reproduce the patched documentation dependency tree with the committed manifest and lockfile. The final audit reports no known vulnerabilities. Next.js is pinned to 16.3.8, Fumadocs core and UI to 16.8.5, and Fumadocs MDX to 14.3.2. Direct PostCSS uses `^8.5.29`.

## Inventory and scope

`gh api repos/xraph/dispatch/dependabot/alerts --paginate` returned 95 open entries: 60 for `docs/pnpm-lock.yaml` and 35 for `docs/package.json`. They represent 60 distinct package/advisory identities across 12 npm packages. Raw alert severity counts were 4 critical, 42 high, 39 medium and 10 low. The baseline `pnpm audit --json` exited 1 and reported 60 advisories, with 2 critical, 29 high, 23 moderate and 6 low findings.

These dependencies belong to the Next.js documentation application. They are outside Dispatch's Go module dependency graph. That does not remove build or server exposure. The app has App Router pages, a server search route, generated Open Graph images and MDX compilation, and its Next.js configuration includes rewrites. The review treats installed versions as applicable until patched or removed; it does not dismiss an advisory because a particular feature was not observed in source.

| Dependency | Dependency path and potential exposure | Final disposition |
| --- | --- | --- |
| next | Direct dependency and Fumadocs peer; development and production documentation server, route generation and image handling. | 16.3.8 |
| sharp | Next.js optional native image dependency; image processing on build/server hosts where installed. | 0.35.5 |
| source-map-js | PostCSS and Tailwind node tooling; source-map parsing during builds. | 1.2.2 |
| postcss-selector-parser | Fumadocs UI through `@fumadocs/tailwind`; build-time selector parsing. | Removed from dependency tree by Fumadocs UI update. |
| image-size | Fumadocs core; documentation image metadata parsing. | Removed from dependency tree by Fumadocs core update. |
| js-yaml | Fumadocs MDX and core; documentation content parsing. | 4.3.2 |
| baseline-browser-mapping | Next.js; browser metadata selection on build/tooling hosts. | 2.11.28 |
| nanoid | PostCSS; identifier generation in CSS tooling. | 3.3.20 |
| postcss | Next.js, Tailwind PostCSS and direct development dependency; CSS parsing and source-map handling. | Next.js 8.5.23; direct/Tailwind 8.5.29. |
| esbuild | Fumadocs MDX; compilation tooling, including development-server capabilities. | 0.28.2 |
| path-to-regexp | Fumadocs core; path pattern compilation. | Removed from dependency tree by Fumadocs core update. |
| picomatch | Fumadocs MDX and tinyglobby/fdir; documentation glob matching. | 4.0.7 |

## Compatibility pin

The only override is `mdast-util-to-markdown: 2.1.2`. It is a compatibility pin, not a vulnerability suppression. Version 2.2.0 uses handler `attention` metadata when serializing emphasis and strong nodes. Fumadocs core 16.8.5 wraps those handlers without preserving that metadata, and the production build recurses until it exceeds the call stack across 29 documentation compilation errors. A core-only pin still mixes 2.1.2 serialization with 2.2.0 handlers supplied by Markdown extensions and does not fix the build. Pinning this single package across the MDX tree to the previous 2.1.2 version preserves processed Markdown output and fixes the build. Every parent accepts the pinned version through its 2.x dependency range.

Remove this pin when a Fumadocs core release preserves the serializer handler metadata or replaces the incompatible stringifier. Verify the change with frozen installation, type generation, lint, the full production build and audit. The final audit includes the pinned version and reports no advisories.

Next.js 16.3.8 accepts patched sharp through `^0.35.4` and ships PostCSS 8.5.23. Fumadocs MDX 14.3.2 accepts esbuild through `^0.28.0`. Its import of `fumadocs-core/content/md/frontmatter` requires a newer core than the original 16.6.3 despite its broad peer range, so core and UI were updated together. The npm registry confirmed every advisory floor in the table below was published. No security override across an incompatible package major is needed.

## Advisory disposition

Each row is one package/advisory identity. Manifest and lockfile entries share the same disposition; affected ranges and first patched versions are the values returned by GitHub, while the final versions come from the regenerated lockfile.

| Package | Advisory | Severity | Affected range | First patched | Final disposition | Raw entries |
| --- | --- | --- | --- | --- | --- | --- |
| baseline-browser-mapping | [GHSA-w5vr-8v7q-w6rv](https://github.com/advisories/GHSA-w5vr-8v7q-w6rv) | medium | `>= 2.0.0, < 2.11.0` | 2.11.0 | 2.11.28 | 1 |
| esbuild | [GHSA-g7r4-m6w7-qqqr](https://github.com/advisories/GHSA-g7r4-m6w7-qqqr) | low | `>= 0.27.3, < 0.28.1` | 0.28.1 | 0.28.2 | 1 |
| image-size | [GHSA-5p2g-fcmc-qvqq](https://github.com/advisories/GHSA-5p2g-fcmc-qvqq) | high | `>= 1.2.0, <= 2.0.2` | 2.0.3 | Removed | 1 |
| image-size | [GHSA-w3rx-r6r6-pgpr](https://github.com/advisories/GHSA-w3rx-r6r6-pgpr) | high | `>= 0.6.3, <= 2.0.2` | 2.0.3 | Removed | 1 |
| js-yaml | [GHSA-2883-xcg3-v3hh](https://github.com/advisories/GHSA-2883-xcg3-v3hh) | high | `>= 4.0.0, < 4.3.2` | 4.3.2 | 4.3.2 | 1 |
| js-yaml | [GHSA-52cp-r559-cp3m](https://github.com/advisories/GHSA-52cp-r559-cp3m) | high | `>= 4.0.0, < 4.3.0` | 4.3.0 | 4.3.2 | 1 |
| js-yaml | [GHSA-5p4m-2wfm-xmqj](https://github.com/advisories/GHSA-5p4m-2wfm-xmqj) | high | `>= 4.0.0, < 4.3.1` | 4.3.1 | 4.3.2 | 1 |
| js-yaml | [GHSA-h67p-54hq-rp68](https://github.com/advisories/GHSA-h67p-54hq-rp68) | medium | `>= 4.0.0, <= 4.1.1` | 4.2.0 | 4.3.2 | 1 |
| nanoid | [GHSA-28wg-ghj8-5hjv](https://github.com/advisories/GHSA-28wg-ghj8-5hjv) | high | `< 3.3.16` | 3.3.16 | 3.3.20 | 1 |
| nanoid | [GHSA-2v37-7h3g-55p8](https://github.com/advisories/GHSA-2v37-7h3g-55p8) | high | `< 3.3.18` | 3.3.18 | 3.3.20 | 1 |
| nanoid | [GHSA-xwg4-73v4-xw9w](https://github.com/advisories/GHSA-xwg4-73v4-xw9w) | high | `< 3.3.12` | 3.3.12 | 3.3.20 | 1 |
| next | [GHSA-267c-6grr-h53f](https://github.com/advisories/GHSA-267c-6grr-h53f) | high | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-26hh-7cqf-hhc6](https://github.com/advisories/GHSA-26hh-7cqf-hhc6) | high | `>= 16.0.0, < 16.2.6` | 16.2.6 | 16.3.8 | 2 |
| next | [GHSA-2xp9-vwfh-vxw4](https://github.com/advisories/GHSA-2xp9-vwfh-vxw4) | critical | `>= 16.0.0, < 16.3.3` | 16.3.3 | 16.3.8 | 2 |
| next | [GHSA-36qx-fr4f-26g5](https://github.com/advisories/GHSA-36qx-fr4f-26g5) | high | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-39w2-rjm5-chcv](https://github.com/advisories/GHSA-39w2-rjm5-chcv) | low | `>= 16.0.0, < 16.3.8` | 16.3.8 | 16.3.8 | 2 |
| next | [GHSA-3g8h-86w9-wvmq](https://github.com/advisories/GHSA-3g8h-86w9-wvmq) | low | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-3x4c-7xq6-9pq8](https://github.com/advisories/GHSA-3x4c-7xq6-9pq8) | medium | `>= 16.0.0-beta.0, < 16.1.7` | 16.1.7 | 16.3.8 | 2 |
| next | [GHSA-4633-3j49-mh5q](https://github.com/advisories/GHSA-4633-3j49-mh5q) | medium | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-492v-c6pp-mqqv](https://github.com/advisories/GHSA-492v-c6pp-mqqv) | high | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-4c39-4ccg-62r3](https://github.com/advisories/GHSA-4c39-4ccg-62r3) | medium | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-4jqv-mc3x-m676](https://github.com/advisories/GHSA-4jqv-mc3x-m676) | medium | `>= 16.0.0, < 16.3.8` | 16.3.8 | 16.3.8 | 2 |
| next | [GHSA-68g3-v927-f742](https://github.com/advisories/GHSA-68g3-v927-f742) | medium | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-6gpp-xcg3-4w24](https://github.com/advisories/GHSA-6gpp-xcg3-4w24) | high | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-89xv-2m56-2m9x](https://github.com/advisories/GHSA-89xv-2m56-2m9x) | high | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-8h8q-6873-q5fj](https://github.com/advisories/GHSA-8h8q-6873-q5fj) | high | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-955p-x3mx-jcvp](https://github.com/advisories/GHSA-955p-x3mx-jcvp) | medium | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-c4j6-fc7j-m34r](https://github.com/advisories/GHSA-c4j6-fc7j-m34r) | high | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-cjq9-62q9-8jv4](https://github.com/advisories/GHSA-cjq9-62q9-8jv4) | high | `>= 16.0.0, < 16.3.8` | 16.3.8 | 16.3.8 | 2 |
| next | [GHSA-f87g-xv8r-7p7x](https://github.com/advisories/GHSA-f87g-xv8r-7p7x) | medium | `>= 16.0.0, < 16.3.8` | 16.3.8 | 16.3.8 | 2 |
| next | [GHSA-ffhc-5mcf-pf4q](https://github.com/advisories/GHSA-ffhc-5mcf-pf4q) | medium | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-ggv3-7p47-pfv8](https://github.com/advisories/GHSA-ggv3-7p47-pfv8) | medium | `>= 16.0.0-beta.0, < 16.1.7` | 16.1.7 | 16.3.8 | 2 |
| next | [GHSA-gx5p-jg67-6x7h](https://github.com/advisories/GHSA-gx5p-jg67-6x7h) | medium | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-h27x-g6w4-24gq](https://github.com/advisories/GHSA-h27x-g6w4-24gq) | medium | `>= 16.0.1, < 16.1.7` | 16.1.7 | 16.3.8 | 2 |
| next | [GHSA-h64f-5h5j-jqjh](https://github.com/advisories/GHSA-h64f-5h5j-jqjh) | medium | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-jcc7-9wpm-mj36](https://github.com/advisories/GHSA-jcc7-9wpm-mj36) | low | `>= 16.0.1, < 16.1.7` | 16.1.7 | 16.3.8 | 2 |
| next | [GHSA-m99w-x7hq-7vfj](https://github.com/advisories/GHSA-m99w-x7hq-7vfj) | high | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-mcj8-r9mp-w47p](https://github.com/advisories/GHSA-mcj8-r9mp-w47p) | medium | `>= 16.0.0, < 16.3.8` | 16.3.8 | 16.3.8 | 2 |
| next | [GHSA-mg66-mrh9-m8jx](https://github.com/advisories/GHSA-mg66-mrh9-m8jx) | high | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-mq59-m269-xvcx](https://github.com/advisories/GHSA-mq59-m269-xvcx) | medium | `>= 16.0.1, < 16.1.7` | 16.1.7 | 16.3.8 | 2 |
| next | [GHSA-p293-qw3h-jr36](https://github.com/advisories/GHSA-p293-qw3h-jr36) | critical | `>= 16.0.0, < 16.3.3` | 16.3.3 | 16.3.8 | 2 |
| next | [GHSA-p9j2-gv94-2wf4](https://github.com/advisories/GHSA-p9j2-gv94-2wf4) | high | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-q4gf-8mx6-v5v3](https://github.com/advisories/GHSA-q4gf-8mx6-v5v3) | high | `>= 16.0.0-beta.0, < 16.2.3` | 16.2.3 | 16.3.8 | 2 |
| next | [GHSA-q8wf-6r8g-63ch](https://github.com/advisories/GHSA-q8wf-6r8g-63ch) | medium | `>= 16.0.0, < 16.2.11` | 16.2.11 | 16.3.8 | 2 |
| next | [GHSA-vfv6-92ff-j949](https://github.com/advisories/GHSA-vfv6-92ff-j949) | low | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| next | [GHSA-wfc6-r584-vfw7](https://github.com/advisories/GHSA-wfc6-r584-vfw7) | medium | `>= 16.0.0, < 16.2.5` | 16.2.5 | 16.3.8 | 2 |
| path-to-regexp | [GHSA-27v5-c462-wpq7](https://github.com/advisories/GHSA-27v5-c462-wpq7) | medium | `>= 8.0.0, < 8.4.0` | 8.4.0 | Removed | 1 |
| path-to-regexp | [GHSA-j3q9-mxjg-w52f](https://github.com/advisories/GHSA-j3q9-mxjg-w52f) | high | `>= 8.0.0, < 8.4.0` | 8.4.0 | Removed | 1 |
| picomatch | [GHSA-3v7f-55p6-f55p](https://github.com/advisories/GHSA-3v7f-55p6-f55p) | medium | `>= 4.0.0, < 4.0.4` | 4.0.4 | 4.0.7 | 1 |
| picomatch | [GHSA-c2c7-rcm5-vvqj](https://github.com/advisories/GHSA-c2c7-rcm5-vvqj) | high | `>= 4.0.0, < 4.0.4` | 4.0.4 | 4.0.7 | 1 |
| postcss | [GHSA-6g55-p6wh-862q](https://github.com/advisories/GHSA-6g55-p6wh-862q) | high | `<= 8.5.11` | 8.5.12 | 8.5.23 / 8.5.29 | 1 |
| postcss | [GHSA-fxqj-rqcc-2cmp](https://github.com/advisories/GHSA-fxqj-rqcc-2cmp) | medium | `<= 8.5.22` | 8.5.23 | 8.5.23 / 8.5.29 | 1 |
| postcss | [GHSA-qx2v-qp2m-jg93](https://github.com/advisories/GHSA-qx2v-qp2m-jg93) | medium | `< 8.5.10` | 8.5.10 | 8.5.23 / 8.5.29 | 1 |
| postcss | [GHSA-r28c-9q8g-f849](https://github.com/advisories/GHSA-r28c-9q8g-f849) | high | `<= 8.5.17` | 8.5.18 | 8.5.23 / 8.5.29 | 1 |
| postcss-selector-parser | [GHSA-rj75-hqrm-r3gf](https://github.com/advisories/GHSA-rj75-hqrm-r3gf) | medium | `< 7.1.6` | 7.1.6 | Removed | 1 |
| postcss-selector-parser | [GHSA-w9m9-85wc-3x92](https://github.com/advisories/GHSA-w9m9-85wc-3x92) | low | `>= 7.1.0, < 7.1.3` | 7.1.3 | Removed | 1 |
| sharp | [GHSA-f88m-g3jw-g9cj](https://github.com/advisories/GHSA-f88m-g3jw-g9cj) | high | `< 0.35.0` | 0.35.0 | 0.35.5 | 1 |
| sharp | [GHSA-rgj7-g3m4-5g8c](https://github.com/advisories/GHSA-rgj7-g3m4-5g8c) | high | `< 0.35.4` | 0.35.4 | 0.35.5 | 1 |
| sharp | [GHSA-wq5f-xc86-pv6w](https://github.com/advisories/GHSA-wq5f-xc86-pv6w) | high | `< 0.35.5` | 0.35.5 | 0.35.5 | 1 |
| source-map-js | [GHSA-68fv-2mgg-jv7q](https://github.com/advisories/GHSA-68fv-2mgg-jv7q) | high | `>= 1.0.0, < 1.2.2` | 1.2.2 | 1.2.2 | 1 |

## Verification

The local default pnpm was 11.5.0, while CI specifies pnpm 10 and the installed tree used pnpm 10. A default-pnpm update failed with `ERR_PNPM_UNEXPECTED_STORE` before changing files. Verification used `corepack pnpm@10` (10.31.0) on Node.js 24.16.0. CI's Node.js 22 execution is not established by these local checks.

Run the docs commands from `docs/`:

```sh
corepack pnpm@10 install --frozen-lockfile
corepack pnpm@10 types:check
corepack pnpm@10 lint
corepack pnpm@10 build
corepack pnpm@10 format package.json
corepack pnpm@10 audit --json
```

The format command targets the owned manifest. Biome does not format Markdown or the pnpm YAML lockfile. The lockfile was generated by pnpm and checked through frozen installation. From the repository root, `make l`, `make f`, `make l` and `make test` were run in that order, with repository-wide formatting coordinated with the controller.

All final commands above passed. The final audit exited 0, returned an empty advisory object and reported zero info, low, moderate, high and critical vulnerabilities. pnpm warned that esbuild's install script was ignored; MDX generation and the production build exercised the installed compiler successfully. These checks establish local installation and build compatibility, not a deployed-server review or exploit reproduction.

A local production server started with `corepack pnpm@10 start --hostname 127.0.0.1 --port 0`. HTTP checks returned 200 for `/`, `/docs/getting-started`, `/api/search?query=job`, `/llms-full.txt`, `/llms.mdx/docs/getting-started` and `/og/docs/getting-started/image.png`. The search response parsed as JSON, the full Markdown export contained the getting-started content, and the image had a PNG signature. Browser layout and deployed-server checks were not performed.

GitHub recalculates Dependabot alerts asynchronously after a push. The clean local audit and patched lockfile do not establish that GitHub has already closed all 95 entries. No alerts were dismissed and no deployment was performed.
