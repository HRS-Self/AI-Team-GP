function isPlainObject(x) {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

function normStr(x) {
  return typeof x === "string" ? x.trim() : "";
}

function uniqSorted(arr) {
  return Array.from(new Set((Array.isArray(arr) ? arr : []).map((x) => String(x || "").trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function hasFile(filesSet, path) {
  return filesSet.has(path);
}

function pickFirstExisting(filesSet, candidates) {
  for (const p of candidates) if (hasFile(filesSet, p)) return p;
  return null;
}

function filesUnderPrefix(files, prefix) {
  const p = String(prefix || "");
  if (!p) return [];
  return (Array.isArray(files) ? files : []).filter((f) => String(f || "").startsWith(p));
}

export function detectLanguages({ repoFiles, packageJson, buildCommands } = {}) {
  const files = uniqSorted(repoFiles);
  const filesSet = new Set(files);
  const out = new Set();

  const hasExt = (ext) => files.some((f) => String(f || "").toLowerCase().endsWith(ext));

  if (filesSet.has("package.json") || isPlainObject(packageJson)) {
    if (hasExt(".ts") || hasExt(".tsx")) out.add("typescript");
    if (hasExt(".js") || hasExt(".jsx") || filesSet.has("next.config.js") || filesSet.has("next.config.mjs")) out.add("javascript");
  }
  if (hasExt(".java") || filesSet.has("pom.xml")) out.add("java");
  if (hasExt(".kt") || filesSet.has("build.gradle.kts")) out.add("kotlin");
  if (files.some((f) => String(f || "").toLowerCase().endsWith(".cs")) || files.some((f) => String(f || "").toLowerCase().endsWith(".csproj"))) out.add("csharp");
  if (files.some((f) => String(f || "").toLowerCase().endsWith(".fs")) || files.some((f) => String(f || "").toLowerCase().endsWith(".fsproj"))) out.add("fsharp");
  if (filesSet.has("go.mod") || hasExt(".go")) out.add("go");
  if (filesSet.has("requirements.txt") || filesSet.has("pyproject.toml") || hasExt(".py")) out.add("python");
  if (filesSet.has("Gemfile") || hasExt(".rb")) out.add("ruby");
  if (filesSet.has("composer.json") || hasExt(".php")) out.add("php");

  const pm = normStr(isPlainObject(buildCommands) ? buildCommands.package_manager : "");
  if (pm === "maven" || pm === "gradle") out.add("java");
  if (pm === "dotnet") out.add("csharp");

  return Array.from(out).sort((a, b) => a.localeCompare(b)).slice(0, 10);
}

export function detectEntrypoints({ repoFiles, packageJson }) {
  const files = uniqSorted(repoFiles);
  const filesSet = new Set(files);
  const out = [];

  if (isPlainObject(packageJson)) {
    const main = normStr(packageJson.main);
    if (main && hasFile(filesSet, main)) out.push(main);

    const bin = packageJson.bin;
    if (typeof bin === "string") {
      const p = normStr(bin);
      if (p && hasFile(filesSet, p)) out.push(p);
    } else if (isPlainObject(bin)) {
      for (const v of Object.values(bin)) {
        const p = normStr(v);
        if (p && hasFile(filesSet, p)) out.push(p);
      }
    }

    const exportsField = packageJson.exports;
    if (typeof exportsField === "string") {
      const p = normStr(exportsField);
      if (p && hasFile(filesSet, p)) out.push(p);
    } else if (isPlainObject(exportsField)) {
      const stack = [exportsField];
      while (stack.length) {
        const cur = stack.pop();
        if (!cur || typeof cur !== "object") continue;
        if (typeof cur === "string") {
          const p = normStr(cur);
          if (p && hasFile(filesSet, p)) out.push(p);
          continue;
        }
        if (Array.isArray(cur)) {
          for (const it of cur) stack.push(it);
          continue;
        }
        for (const v of Object.values(cur)) stack.push(v);
      }
    }
  }

  const conventional = [
    "src/index.ts",
    "src/index.js",
    "src/server.ts",
    "src/server.js",
    "src/app.ts",
    "src/app.js",
    "index.ts",
    "index.js",
    "server.ts",
    "server.js",
    "app.ts",
    "app.js",
    "src/main.ts",
    "src/main.tsx",
    "src/main.js",
    "src/main.jsx",
    "src/App.tsx",
    "src/App.jsx",
    "src/App.ts",
    "src/App.js",
    "next.config.js",
    "next.config.mjs",
    "vite.config.ts",
    "vite.config.js",
    "webpack.config.js",
    "webpack.config.ts",
    "Dockerfile",
    "docker-compose.yml",
    "docker-compose.yaml",
  ];
  for (const p of conventional) if (hasFile(filesSet, p)) out.push(p);

  // Java/Kotlin conventions (Spring Boot / general JVM apps)
  for (const f of files) {
    if (!/^src\/main\/(java|kotlin)\//i.test(f)) continue;
    if (/(^|\/)[A-Za-z0-9_]+Application\.(java|kt)$/i.test(f)) out.push(f);
    if (/(^|\/)Main\.(java|kt)$/i.test(f)) out.push(f);
  }
  for (const p of ["pom.xml", "build.gradle", "build.gradle.kts"]) if (hasFile(filesSet, p)) out.push(p);

  // .NET conventions (ASP.NET / console apps)
  for (const p of ["Program.cs", "Startup.cs", "global.json"]) if (hasFile(filesSet, p)) out.push(p);
  for (const f of files) {
    const lower = f.toLowerCase();
    if (lower.endsWith(".sln")) out.push(f);
    if (lower.endsWith(".csproj") || lower.endsWith(".fsproj") || lower.endsWith(".vbproj")) out.push(f);
  }

  const pagesAny = filesUnderPrefix(files, "pages/");
  if (pagesAny.length) out.push(pickFirstExisting(filesSet, ["pages/_app.tsx", "pages/_app.jsx", "pages/_app.ts", "pages/_app.js", pagesAny[0]]));

  const appAny = filesUnderPrefix(files, "app/");
  if (appAny.length) out.push(pickFirstExisting(filesSet, ["app/layout.tsx", "app/layout.jsx", "app/layout.ts", "app/layout.js", appAny[0]]));

  const helmChart = pickFirstExisting(filesSet, ["helm/Chart.yaml", "helm/Chart.yml"]);
  if (helmChart) out.push(helmChart);

  const workflowsAny = filesUnderPrefix(files, ".github/workflows/");
  if (workflowsAny.length) out.push(workflowsAny[0]);

  return uniqSorted(out).filter((x) => x && typeof x === "string");
}

export function detectApiSurface({ repoFiles }) {
  const files = uniqSorted(repoFiles);

  const openapi_files = uniqSorted(
    files.filter((f) => {
      const lower = String(f || "").toLowerCase();
      if (/(^|\/)(openapi|swagger)\.(ya?ml|json)$/.test(lower)) return true;
      if (lower.endsWith(".graphql") || lower.endsWith(".gql")) return true;
      if (lower.endsWith(".proto")) return true;
      return false;
    }),
  ).slice(0, 200);

  const routes_controllers = uniqSorted(
    files.filter((f) => {
      const lower = String(f || "").toLowerCase();
      if (/(^|\/)(routes|controllers|router)(\/|$)/.test(lower)) return true;
      if (/(^|\/)(pages\/api|app\/api)(\/|$)/.test(lower)) return true;
      if (/(^|\/)[a-z0-9_]+controller\.(java|kt|cs)$/.test(lower)) return true;
      return false;
    }),
  ).slice(0, 200);

  const events_topics = uniqSorted(
    files.filter((f) => {
      const lower = String(f || "").toLowerCase();
      if (/(^|\/)(events|consumers|producers|queues|topics)(\/|$)/.test(lower)) return true;
      if (/(^|\/)(kafka|rabbitmq|amqp)(\/|$)/.test(lower)) return true;
      if (/(^|\/)(kafka|rabbitmq|amqp)[^/]*\.(ya?ml|json|properties)$/.test(lower)) return true;
      return false;
    }),
  ).slice(0, 200);

  return { openapi_files, routes_controllers, events_topics };
}

export function detectHotspots({ repoFiles, entrypoints, apiSurface }) {
  const files = uniqSorted(repoFiles);
  const filesSet = new Set(files);
  const out = [];

  for (const p of uniqSorted(entrypoints)) out.push({ file_path: p, reason: "entrypoint" });

  const configRoots = uniqSorted(
    files.filter((f) => {
      const lower = f.toLowerCase();
      return (
        lower === ".env.example" ||
        lower === ".npmrc" ||
        lower === ".yarnrc" ||
        lower.startsWith(".eslintrc") ||
        lower === "tsconfig.json" ||
        lower.startsWith("tsconfig.") ||
        lower.startsWith("tailwind.config.") ||
        lower === "postcss.config.js" ||
        lower === "postcss.config.cjs" ||
        lower === "jest.config.js" ||
        lower === "jest.config.ts"
      );
    }),
  );
  for (const p of configRoots) out.push({ file_path: p, reason: "config" });

  const schemaFiles = uniqSorted(files.filter((f) => /(^|\/)(openapi|swagger)\.(ya?ml|json)$/i.test(f) || f.toLowerCase().endsWith(".graphql") || f.toLowerCase().endsWith(".proto")));
  for (const p of schemaFiles) out.push({ file_path: p, reason: "schema" });

  const infraFiles = uniqSorted(
    [
      ...["Dockerfile", "docker-compose.yml", "docker-compose.yaml", "helm/Chart.yaml", "helm/Chart.yml"].filter((p) => hasFile(filesSet, p)),
      ...files.filter((f) => f.startsWith(".github/workflows/")),
      ...files.filter((f) => f.startsWith("k8s/") || f.startsWith("kubernetes/")),
    ].filter(Boolean),
  );
  for (const p of infraFiles) out.push({ file_path: p, reason: "infra" });

  const apiBoundaryFiles = uniqSorted(files.filter((f) => /(^|\/)(routes|controllers|router|app\/api)(\/|$)/i.test(f)));
  if (apiBoundaryFiles.length) out.push({ file_path: apiBoundaryFiles[0], reason: "api_surface" });
  const apiObj = isPlainObject(apiSurface) ? apiSurface : {};
  const apiCandidates = [
    ...(Array.isArray(apiObj.openapi_files) ? apiObj.openapi_files : []),
    ...(Array.isArray(apiObj.routes_controllers) ? apiObj.routes_controllers : []),
    ...(Array.isArray(apiObj.events_topics) ? apiObj.events_topics : []),
  ];
  for (const p of uniqSorted(apiCandidates).filter((p) => hasFile(filesSet, p))) out.push({ file_path: p, reason: "api_surface" });

  const dedup = new Map();
  for (const h of out) {
    const k = `${h.file_path}::${h.reason}`;
    if (!dedup.has(k)) dedup.set(k, { file_path: h.file_path, reason: h.reason });
  }
  return Array.from(dedup.values()).sort((a, b) => `${a.file_path}::${a.reason}`.localeCompare(`${b.file_path}::${b.reason}`));
}

export function selectFingerprintFiles({ repoFiles }) {
  const files = uniqSorted(repoFiles);
  const filesSet = new Set(files);

  const picks = [];
  const add = (path, category) => {
    const p = String(path || "").trim();
    const c = String(category || "").trim();
    if (!p || !c) return;
    if (!filesSet.has(p)) return;
    picks.push({ path: p, category: c });
  };

  // package manifests / lockfiles
  for (const p of [
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "pom.xml",
    "build.gradle",
    "build.gradle.kts",
    "go.mod",
    "Cargo.toml",
    "requirements.txt",
    "pyproject.toml",
    "composer.json",
    "Gemfile",
  ]) {
    add(p, "package_manifest");
  }

  // contracts / schemas
  for (const f of files) {
    const lower = f.toLowerCase();
    if (/(^|\/)(openapi|swagger)\.(ya?ml|json)$/.test(lower)) add(f, "api_contract");
    if (lower.endsWith(".graphql") || lower.endsWith(".gql")) add(f, "api_contract");
    if (lower.endsWith(".proto")) add(f, "api_contract");
    if (lower === "prisma/schema.prisma") add(f, "schema");
  }

  // migrations
  for (const f of files) {
    const lower = f.toLowerCase();
    if (lower.startsWith("migrations/") || lower.startsWith("db/migrations/") || lower.startsWith("prisma/migrations/")) add(f, "migration");
    if (lower.startsWith("src/main/resources/db/migration/") || lower.startsWith("src/main/resources/db/migrations/")) add(f, "migration");
    if (lower.startsWith("src/main/resources/db/changelog/") || lower.startsWith("liquibase/") || lower.startsWith("flyway/")) add(f, "migration");
  }

  // config roots
  for (const p of [
    ".env.example",
    ".npmrc",
    ".yarnrc",
    "tsconfig.json",
    "next.config.js",
    "next.config.mjs",
    "vite.config.ts",
    "vite.config.js",
    "webpack.config.js",
    "webpack.config.ts",
    "tailwind.config.js",
    "tailwind.config.ts",
    "postcss.config.js",
    "postcss.config.cjs",
  ]) {
    add(p, "config");
  }
  for (const f of files) if (f.toLowerCase().startsWith(".eslintrc")) add(f, "config");

  // infra
  for (const p of ["Dockerfile", "docker-compose.yml", "docker-compose.yaml", "helm/Chart.yaml", "helm/Chart.yml"]) add(p, "infra");
  for (const f of files) if (f.startsWith(".github/workflows/")) add(f, "infra");

  // public API barrels
  for (const p of ["src/index.ts", "src/index.js", "index.ts", "index.js"]) add(p, "source");
  // Common build system roots (help incremental refresh and completeness)
  for (const p of ["gradlew", "settings.gradle", "settings.gradle.kts", "Directory.Build.props", "Directory.Build.targets"]) add(p, "package_manifest");
  for (const f of files) {
    const lower = f.toLowerCase();
    if (lower.endsWith(".sln") || lower.endsWith(".csproj") || lower.endsWith(".fsproj") || lower.endsWith(".vbproj")) add(f, "package_manifest");
  }

  const map = new Map();
  for (const it of picks) map.set(it.path, it.category);
  const out = Array.from(map.entries()).map(([path, category]) => ({ path, category }));
  out.sort((a, b) => `${a.category}::${a.path}`.localeCompare(`${b.category}::${b.path}`));
  return out;
}

export function detectMigrationsSchema({ repoFiles }) {
  const files = uniqSorted(repoFiles);
  const out = [];
  for (const f of files) {
    const lower = String(f || "").toLowerCase();
    if (lower === "prisma/schema.prisma") out.push(f);
    if (lower.startsWith("prisma/migrations/")) out.push(f);
    if (lower.startsWith("migrations/")) out.push(f);
    if (lower.startsWith("db/migrations/")) out.push(f);
    if (lower.startsWith("src/main/resources/db/migration/") || lower.startsWith("src/main/resources/db/migrations/")) out.push(f);
    if (lower.startsWith("src/main/resources/db/changelog/")) out.push(f);
    if (lower.startsWith("liquibase/") || lower.startsWith("flyway/")) out.push(f);
    if (/(^|\/)(schema|schemas)(\/|$)/.test(lower) && (lower.endsWith(".sql") || lower.endsWith(".prisma") || lower.endsWith(".graphql") || lower.endsWith(".proto"))) out.push(f);
  }
  return uniqSorted(out).slice(0, 200);
}

export function detectBuildCommands({ repoFiles, packageJson }) {
  const files = uniqSorted(repoFiles);
  const filesSet = new Set(files);

  const evidence_files = [];
  const addEvidence = (p) => {
    const s = normStr(p);
    if (!s) return;
    if (filesSet.has(s)) evidence_files.push(s);
  };

  let package_manager = "unknown";
  if (filesSet.has("pnpm-lock.yaml")) package_manager = "pnpm";
  else if (filesSet.has("yarn.lock")) package_manager = "yarn";
  else if (filesSet.has("package-lock.json")) package_manager = "npm";
  else if (filesSet.has("package.json")) package_manager = "npm";
  else if (filesSet.has("pom.xml")) package_manager = "maven";
  else if (filesSet.has("build.gradle") || filesSet.has("build.gradle.kts")) package_manager = "gradle";
  else if (files.some((f) => {
    const lower = String(f || "").toLowerCase();
    return lower.endsWith(".sln") || lower.endsWith(".csproj") || lower.endsWith(".fsproj") || lower.endsWith(".vbproj");
  })) {
    package_manager = "dotnet";
  }

  for (const p of ["package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock", "pom.xml", "build.gradle", "build.gradle.kts"]) addEvidence(p);
  for (const f of files) {
    const lower = String(f || "").toLowerCase();
    if (lower.endsWith(".sln") || lower.endsWith(".csproj") || lower.endsWith(".fsproj") || lower.endsWith(".vbproj")) addEvidence(f);
  }

  const scripts = {};
  if (isPlainObject(packageJson) && isPlainObject(packageJson.scripts)) {
    const entries = Object.entries(packageJson.scripts)
      .map(([k, v]) => [normStr(k), normStr(v)])
      .filter(([k, v]) => k && v);
    entries.sort((a, b) => a[0].localeCompare(b[0]));
    for (const [k, v] of entries) scripts[k] = v;
  }

  const install = [];
  if (package_manager === "npm") install.push(filesSet.has("package-lock.json") ? "npm ci" : "npm install");
  if (package_manager === "yarn") install.push("yarn install");
  if (package_manager === "pnpm") install.push("pnpm install");

  const lint = [];
  const build = [];
  const test = [];
  if (package_manager === "npm" || package_manager === "yarn" || package_manager === "pnpm") {
    const runner = package_manager === "yarn" ? "yarn" : package_manager;
    if (scripts.lint) lint.push(`${runner} run lint`);
    if (scripts.build) build.push(`${runner} run build`);
    if (scripts.test) test.push(`${runner} run test`);
  }

  return {
    package_manager,
    install: uniqSorted(install),
    lint: uniqSorted(lint),
    build: uniqSorted(build),
    test: uniqSorted(test),
    scripts,
    evidence_files: uniqSorted(evidence_files),
  };
}

export function detectCrossRepoDependenciesFromPackageJson({ packageJson }) {
  const pkg = isPlainObject(packageJson) ? packageJson : null;
  if (!pkg) return [];

  const deps = [];
  const visit = (obj) => {
    if (!isPlainObject(obj)) return;
    for (const [nameRaw, verRaw] of Object.entries(obj)) {
      const name = normStr(nameRaw);
      const ver = normStr(verRaw);
      if (!name || !ver) continue;
      let type = "npm";
      const lower = ver.toLowerCase();
      if (lower.startsWith("git+") || lower.includes("github.com") || lower.endsWith(".git") || lower.startsWith("github:")) type = "git";
      else if (lower.startsWith("http://") || lower.startsWith("https://")) type = "http";
      else if (lower.startsWith("file:") || lower.startsWith("link:") || lower.startsWith("workspace:")) type = "npm";
      else continue; // Only treat non-registry references as cross-repo dependencies.
      deps.push({ type, target: `${name}@${ver}`, evidence_refs: ["package.json"] });
    }
  };
  visit(pkg.dependencies);
  visit(pkg.devDependencies);
  visit(pkg.peerDependencies);
  visit(pkg.optionalDependencies);

  // Dedupe stable.
  const map = new Map();
  for (const d of deps) {
    const k = `${d.type}::${d.target}`;
    if (!map.has(k)) map.set(k, d);
  }
  return Array.from(map.values()).sort((a, b) => `${a.type}::${a.target}`.localeCompare(`${b.type}::${b.target}`));
}
