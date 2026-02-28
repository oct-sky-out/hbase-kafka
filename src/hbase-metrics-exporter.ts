import { createServer } from "node:http";
import { Gauge, Histogram, Registry } from "prom-client";

type HBaseJmxResponse = {
  beans?: Array<Record<string, unknown>>;
};

const hbaseJmxUrl = process.env.HBASE_JMX_URL ?? "http://localhost:16010/jmx";
const metricsPort = Number.parseInt(process.env.HBASE_METRICS_PORT ?? "9466", 10);

const registry = new Registry();
const hbaseJmxUp = new Gauge({
  name: "hbase_jmx_up",
  help: "Whether HBase JMX endpoint is reachable (1=up, 0=down)",
  registers: [registry]
});
const hbaseMasterRegionServers = new Gauge({
  name: "hbase_master_region_servers",
  help: "Number of live HBase region servers",
  registers: [registry]
});
const hbaseMasterDeadRegionServers = new Gauge({
  name: "hbase_master_dead_region_servers",
  help: "Number of dead HBase region servers",
  registers: [registry]
});
const hbaseMasterAverageLoad = new Gauge({
  name: "hbase_master_average_load",
  help: "HBase master average load",
  registers: [registry]
});
const hbaseMasterRitCount = new Gauge({
  name: "hbase_master_rit_count",
  help: "HBase regions in transition count",
  registers: [registry]
});
const hbaseJmxScrapeDurationMs = new Histogram({
  name: "hbase_jmx_scrape_duration_ms",
  help: "HBase JMX scrape duration in milliseconds",
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2000],
  registers: [registry]
});

function findBean(beans: Array<Record<string, unknown>>, nameIncludes: string[]): Record<string, unknown> | undefined {
  return beans.find((bean) => {
    const beanName = typeof bean.name === "string" ? bean.name : "";
    return nameIncludes.some((needle) => beanName.includes(needle));
  });
}

function readNumber(bean: Record<string, unknown> | undefined, keys: string[]): number | undefined {
  if (!bean) {
    return undefined;
  }

  for (const key of keys) {
    const value = bean[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }

  return undefined;
}

async function refreshHBaseMetrics(): Promise<void> {
  const endTimer = hbaseJmxScrapeDurationMs.startTimer();
  try {
    const response = await fetch(hbaseJmxUrl);
    if (!response.ok) {
      throw new Error(`JMX response status=${response.status}`);
    }

    const payload = (await response.json()) as HBaseJmxResponse;
    const beans = payload.beans ?? [];
    const masterBean =
      findBean(beans, ["name=Master,sub=Server", "name=Master,sub=AssignmentManager"]) ??
      findBean(beans, ["name=Master"]);

    const liveRegionServers = readNumber(masterBean, ["numRegionServers", "NumRegionServers"]);
    const deadRegionServers = readNumber(masterBean, ["numDeadRegionServers", "NumDeadRegionServers"]);
    const averageLoad = readNumber(masterBean, ["averageLoad", "AverageLoad"]);
    const ritCount = readNumber(masterBean, ["ritCount", "RITCount", "NumRegionsInTransition"]);

    if (liveRegionServers !== undefined) {
      hbaseMasterRegionServers.set(liveRegionServers);
    }
    if (deadRegionServers !== undefined) {
      hbaseMasterDeadRegionServers.set(deadRegionServers);
    }
    if (averageLoad !== undefined) {
      hbaseMasterAverageLoad.set(averageLoad);
    }
    if (ritCount !== undefined) {
      hbaseMasterRitCount.set(ritCount);
    }

    hbaseJmxUp.set(1);
  } catch (error) {
    hbaseJmxUp.set(0);
    throw error;
  } finally {
    endTimer();
  }
}

function startServer(): void {
  if (Number.isNaN(metricsPort) || metricsPort <= 0) {
    throw new Error(`Invalid HBASE_METRICS_PORT=${process.env.HBASE_METRICS_PORT}`);
  }

  const server = createServer(async (request, response) => {
    if (request.url !== "/metrics") {
      response.statusCode = 404;
      response.end("Not Found");
      return;
    }

    try {
      await refreshHBaseMetrics();
      response.statusCode = 200;
      response.setHeader("Content-Type", registry.contentType);
      response.end(await registry.metrics());
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      response.statusCode = 500;
      response.end(`HBase metrics scrape failed: ${message}`);
    }
  });

  server.listen(metricsPort, () => {
    console.log(`HBase metrics exporter listening on :${metricsPort}/metrics`);
    console.log(`Scraping JMX from ${hbaseJmxUrl}`);
  });
}

startServer();
