export function routeDomainToTeam(domain) {
  if (domain === "backend" || domain === "idp") return "backend";
  if (domain === "portal") return "portal";
  if (domain.startsWith("mobile:")) return "mobile";
  if (domain === "devops") return "devops";
  if (domain === "qa") return "qa";
  return "backend";
}

