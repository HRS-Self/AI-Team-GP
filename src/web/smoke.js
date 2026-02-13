const base = process.env.WEB_URL || "http://127.0.0.1:8090";
const passcode = process.env.WEB_PASSCODE || "";

async function main() {
  if (!passcode) {
    throw new Error("Missing WEB_PASSCODE for smoke test.");
  }

  const jar = [];
  const req = async (path, { method = "GET", body = null } = {}) => {
    const headers = { "Content-Type": "application/json" };
    if (jar.length) headers.Cookie = jar.join("; ");
    const res = await fetch(`${base}${path}`, { method, headers, body: body ? JSON.stringify(body) : undefined });
    const setCookie = res.headers.get("set-cookie");
    if (setCookie) {
      const cookie = setCookie.split(";")[0];
      jar.push(cookie);
    }
    const json = await res.json().catch(() => ({}));
    return { status: res.status, ok: res.ok, json };
  };

  const login = await req("/api/login", { method: "POST", body: { passcode } });
  if (!login.ok) throw new Error(`Login failed (${login.status}): ${JSON.stringify(login.json)}`);

  const portfolio = await req("/api/run-command", { method: "POST", body: { cmd: "--portfolio", args: {} } });
  if (!portfolio.ok || portfolio.json?.ok !== true) {
    throw new Error(`Portfolio failed (${portfolio.status}): ${JSON.stringify(portfolio.json)}`);
  }

  process.stdout.write("web smoke ok\n");
}

main().catch((err) => {
  process.stderr.write(`web smoke failed: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
