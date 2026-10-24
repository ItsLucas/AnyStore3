// Daily maintenance trigger for AnyStore on CloudBase Run.
//
// The service scales to zero, so no in-process timer is alive at the scheduled
// time. A timer trigger runs this function, whose request wakes an instance and
// runs maintenance there. Passes are idempotent and resumable, so retries and
// a missed day are harmless.
//
// Environment:
//   ANYSTORE_MAINTENANCE_URL  e.g. https://<service-domain>/internal/maintenance
//   ANYSTORE_AUTH_TOKEN       the service's API token

// A cold start can make the gateway answer 502/503/504 or drop the connection.
const MAX_ATTEMPTS = 5;
const RETRY_DELAY_MS = 10_000;
const REQUEST_TIMEOUT_MS = 60_000;
// Each pass has a time budget; keep calling while it reports leftover work.
const MAX_PASSES = 6;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runPass(url, token) {
  for (let attempt = 1; ; attempt++) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      const body = await response.text();
      console.log(`attempt ${attempt}: ${response.status} ${body.slice(0, 500)}`);
      if (response.ok) {
        return JSON.parse(body);
      }
      if (![502, 503, 504].includes(response.status) || attempt >= MAX_ATTEMPTS) {
        throw new Error(`maintenance trigger failed with ${response.status}`);
      }
    } catch (error) {
      if (attempt >= MAX_ATTEMPTS || error.message.startsWith("maintenance trigger failed")) {
        throw error;
      }
      console.log(`attempt ${attempt}: ${error.message}`);
    }
    await sleep(RETRY_DELAY_MS);
  }
}

exports.main = async () => {
  const url = process.env.ANYSTORE_MAINTENANCE_URL;
  const token = process.env.ANYSTORE_AUTH_TOKEN;
  if (!url || !token) {
    throw new Error("ANYSTORE_MAINTENANCE_URL and ANYSTORE_AUTH_TOKEN must be set");
  }

  const reports = [];
  for (let pass = 1; pass <= MAX_PASSES; pass++) {
    const report = await runPass(url, token);
    reports.push(report);
    if (report.status !== "completed" || report.drained) {
      break;
    }
  }
  return reports;
};
