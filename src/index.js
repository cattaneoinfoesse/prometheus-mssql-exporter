const appLog = require("debug")("app");
const dbLog = require("debug")("db");
const queriesLog = require("debug")("queries");

const sql = require("mssql");
const app = require("express")();
const client = require("prom-client");

const { getMetrics } = require("./metrics");

if (!process.env["CONNECTION_STRINGS"]) {
  throw new Error("Missing CONNECTION_STRINGS information");
}

let config = {
  connectStrings: process.env["CONNECTION_STRINGS"].split("|").map(connectString => {
    appLog(`Parsing connection string: ${connectString}`);
    const config = sql.ConnectionPool.parseConnectionString(connectString);
    config.arrayRowMode = true;
    // config.trustServerCertificate = true;
    config.options.arrayRowMode = true;
    config.options.rowCollectionOnRequestCompletion = true;
    return config;
  }),
  port: parseInt(process.env["EXPOSE"]) || 4000
};

/**
 * Connects to a database server.
 *
 * @param connectionConfig {Object} connection config
 *
 * @returns Promise<sql.ConnectionPool>
 */
async function connect(connectionConfig) {
  dbLog("Connecting");
  const pool = new sql.ConnectionPool(connectionConfig);
  await pool.connect();
  return pool;
}

/**
 * Recursive function that executes all collectors sequentially
 *
 * @param connection {sql.ConnectionPool} database connection
 * @param collector {Object} single metric: {query: string, collect: function(rows, metric)}
 * @param name {string} name of collector variable
 *
 * @returns Promise of collect operation (no value returned)
 */
async function measure(connection, collector, name) {
  return new Promise(async (resolve) => {
    queriesLog(`Executing metric '${name}' query: ${collector.query}`);
    await connection.request().query(collector.query).then(result => {
      if (result.recordset.length > 0) {
        try {
          collector.collect(result.recordset, collector.metrics, connection.config.server);
        } catch (error) {
          console.error(`Error processing metric '${name}' data`, collector.query, JSON.stringify(result.recordset), error);
        }
        resolve();
      } else {
        console.error(`No results executing metric '${name}' SQL query`, collector.query);
        resolve();
      }
    }).catch(error => {
      console.error(`Error executing metric '${name}' SQL query`, collector.query, error);
      resolve();
    });
  });
}

/**
 * Function that collects from an active server.
 *
 * @param connection database connection
 * @param entries array of metrics
 *
 * @returns Promise of execution (no value returned)
 */
async function collect(connection, entries) {
  const tasks = []
  for (const [metricName, metric] of Object.entries(entries)) {
    tasks.push(measure(connection, metric, metricName));
  }
  await Promise.all(tasks);
}

app.get("/", (req, res) => {
  res.redirect("/metrics");
});

const entries = getMetrics();


app.get("/metrics", async (req, res) => {
  res.contentType(client.register.contentType);
  appLog("Received /metrics request");
  const tasks = []
  for (const connectionString of config.connectStrings) {
    try {
      let connection = await connect(connectionString);
      tasks.push(collect(connection, entries).finally(async () =>  await connection.close()))
    } catch (error) {
      // error connecting
      appLog(`Error handling /metrics request for server '${connectionString.server}'`, error);
      const mssqlUp = entries.mssql_up.metrics.mssql_up;
      mssqlUp.set({ host: connectionString.server }, 0);
    }
  }
  await Promise.all(tasks);
  appLog("Successfully processed /metrics request");
  res.send(client.register.metrics());
});

const server = app.listen(config.port, function() {
  appLog(
    `Prometheus-MSSQL Exporter listening`
  );
});

process.on("SIGINT", function() {
  server.close();
  process.exit(0);
});
