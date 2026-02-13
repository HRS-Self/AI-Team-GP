export class ContractValidationError extends Error {
  constructor({ message, path }) {
    super(String(message || "Contract validation failed"));
    this.name = "ContractValidationError";
    this.path = String(path || "$");
  }
}

export function fail(path, reason) {
  const p = String(path || "$");
  const r = String(reason || "invalid");
  throw new ContractValidationError({ path: p, message: `${p}: ${r}` });
}

