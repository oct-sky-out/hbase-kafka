const hbase = require("hbase") as (config: { host: string; port: number }) => {
  table: (name: string) => {
    scan: (
      options: {
        startRow?: string;
        column?: string;
        maxVersions?: number;
        filter?: { type: "PageFilter"; value: number };
      },
      callback: (error: unknown, cells: Array<{ key: string; column: string; timestamp: number; $: string }>) => void
    ) => void;
  };
};

const tableName = process.env.HBASE_TABLE ?? "audit:logs";
const columnFamily = process.env.HBASE_CF ?? "cf";
const hbaseHost = process.env.HBASE_HOST ?? "localhost";
const hbasePort = Number.parseInt(process.env.HBASE_PORT ?? "9090", 10);
const limit = 50;
const scanBatchSize = limit + 1;

const client = hbase({
  host: hbaseHost,
  port: Number.isNaN(hbasePort) ? 9090 : hbasePort
});

type Cell = {
  key: string;
  column: string;
  timestamp: number;
  $: string;
};

function parseStartRowArg(argv: string[]): string | undefined {
  const index = argv.findIndex((value) => value === "--start-row");
  if (index === -1) {
    return undefined;
  }
  return argv[index + 1];
}

function groupCellsByRow(cells: Cell[]): {
  rowsByKey: Record<string, Record<string, string>>;
  rowOrder: string[];
} {
  const rowsByKey: Record<string, Record<string, string>> = {};
  const rowOrder: string[] = [];

  for (const cell of cells) {
    if (!rowsByKey[cell.key]) {
      rowsByKey[cell.key] = {};
      rowOrder.push(cell.key);
    }
    rowsByKey[cell.key][cell.column] = cell.$;
  }

  return { rowsByKey, rowOrder };
}

function printTable(rowsByKey: Record<string, Record<string, string>>, rowKeys: string[]): void {
  if (rowKeys.length === 0) {
    console.log("No rows found.");
    return;
  }

  const columns = new Set<string>();
  for (const rowKey of rowKeys) {
    for (const column of Object.keys(rowsByKey[rowKey])) {
      columns.add(column);
    }
  }

  const orderedColumns = Array.from(columns).sort();
  const rows = rowKeys.map((rowKey) => {
    const row: Record<string, string> = { row_key: rowKey };
    for (const column of orderedColumns) {
      row[column] = rowsByKey[rowKey][column] ?? "";
    }
    return row;
  });

  console.table(rows);
}

function runScan(): Promise<void> {
  return new Promise((resolve, reject) => {
    const startRow = parseStartRowArg(process.argv.slice(2));
    const scanOptions: {
      startRow?: string;
      column: string;
      maxVersions: number;
      filter: { type: "PageFilter"; value: number };
    } = {
      column: columnFamily,
      maxVersions: 1,
      filter: {
        type: "PageFilter",
        value: scanBatchSize
      }
    };

    if (startRow) {
      scanOptions.startRow = startRow;
    }

    client.table(tableName).scan(
      scanOptions,
      (error, cells) => {
        if (error) {
          reject(error);
          return;
        }

        const { rowsByKey, rowOrder } = groupCellsByRow(cells ?? []);
        const displayRowKeys = rowOrder.slice(0, limit);
        const displayRows = displayRowKeys.reduce<Record<string, Record<string, string>>>((acc, rowKey) => {
          acc[rowKey] = rowsByKey[rowKey];
          return acc;
        }, {});

        printTable(displayRows, displayRowKeys);

        if (rowOrder.length > limit) {
          const nextStartRow = rowOrder[limit];
          console.log(`\nnext_start_row=${nextStartRow}`);
          console.log(`Run next page: pnpm hbase:scan -- --start-row \"${nextStartRow}\"`);
        }
        resolve();
      }
    );
  });
}

runScan().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`HBase scan failed: ${message}`);
  process.exit(1);
});
