/**
 * Collection of metrics and their associated SQL requests
 * Created by Pierre Awaragi
 */
const metricsLog = require("debug")("metrics");
const client = require("prom-client");
const { productVersionParse } = require("./utils");

function getMetrics() {

  const mssql_up = {
    metrics: {
      mssql_up: new client.Gauge({
        name: "mssql_up",
        help: "UP Status",
        labelNames: ["host"]
      })
    },
    query: "SELECT 1",
    collect: (rows, metrics, host) => {
      let mssql_up = rows[0][0];
      metricsLog("Fetched status of instance", host, mssql_up);
      metrics.mssql_up.set({ host }, mssql_up);
    }
  };

  const mssql_product_version = {
    metrics: {
      mssql_product_version: new client.Gauge({
        name: "mssql_product_version",
        help: "Instance version (Major.Minor)",
        labelNames: ["host"]
      })
    },
    query: `SELECT CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) AS ProductVersion,
          SERVERPROPERTY('ProductVersion') AS ProductVersion
        `,
    collect: (rows, metrics, host) => {
      let v = productVersionParse(rows[0][0]);
      const mssql_product_version = v.major + "." + v.minor;
      metricsLog("Fetched version of instance", host, mssql_product_version);
      metrics.mssql_product_version.set({ host }, mssql_product_version);
    }
  };

  const mssql_instance_local_time = {
    metrics: {
      mssql_instance_local_time: new client.Gauge({
        name: "mssql_instance_local_time",
        help: "Number of seconds since epoch on local instance",
        labelNames: ["host"]
      })
    },
    query: `SELECT DATEDIFF(second, '19700101', GETUTCDATE())`,
    collect: (rows, metrics, host) => {
      const mssql_instance_local_time = rows[0][0];
      metricsLog("Fetched current time", mssql_instance_local_time);
      metrics.mssql_instance_local_time.set({ host }, mssql_instance_local_time);
    }
  };

  const mssql_connections = {
    metrics: {
      mssql_connections: new client.Gauge({
        name: "mssql_connections",
        help: "Number of active connections",
        labelNames: ["host", "database", "state"]
      })
    },
    query: `SELECT DB_NAME(sP.dbid)
                 , COUNT(sP.spid)
            FROM sys.sysprocesses sP
            GROUP BY DB_NAME(sP.dbid)`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const mssql_connections = row[1];
        metricsLog("Fetched number of connections for database", database, mssql_connections);
        metrics.mssql_connections.set({ host, database, state: "current" }, mssql_connections);
      }
    }
  };

  const mssql_client_connections = {
    metrics: {
      mssql_client_connections: new client.Gauge({
        name: "mssql_client_connections",
        help: "Number of active client connections",
        labelNames: ["host", "client", "database"]
      })
    },
    query: `SELECT host_name, DB_NAME(dbid) dbname, COUNT(*) session_count
            FROM sys.dm_exec_sessions a
                     LEFT JOIN sysprocesses b on a.session_id = b.spid
            WHERE is_user_process = 1
            GROUP BY host_name, dbid`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const client = row[0];
        const database = row[1];
        const mssql_client_connections = row[2];
        metricsLog("Fetched number of connections for client", host, client, database, mssql_client_connections);
        metrics.mssql_client_connections.set({ host, client, database }, mssql_client_connections);
      }
    }
  };

  const mssql_deadlocks = {
    metrics: {
      mssql_deadlocks_per_second: new client.Gauge({
        name: "mssql_deadlocks",
        help: "Number of lock requests per second that resulted in a deadlock since last restart",
        labelNames: ["host"]
      })
    },
    query: `SELECT cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Number of Deadlocks/sec'
              AND instance_name = '_Total'`,
    collect: (rows, metrics, host) => {
      const mssql_deadlocks = rows[0][0];
      metricsLog("Fetched number of deadlocks/sec", host, mssql_deadlocks);
      metrics.mssql_deadlocks_per_second.set({ host }, mssql_deadlocks);
    }
  };

  const mssql_user_errors = {
    metrics: {
      mssql_user_errors: new client.Gauge({
        name: "mssql_user_errors",
        help: "Number of user errors/sec since last restart",
        labelNames: ["host"]
      })
    },
    query: `SELECT cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Errors/sec'
              AND instance_name = 'User Errors'`,
    collect: (rows, metrics, host) => {
      const mssql_user_errors = rows[0][0];
      metricsLog("Fetched number of user errors/sec", host, mssql_user_errors);
      metrics.mssql_user_errors.set({ host }, mssql_user_errors);
    }
  };

  const mssql_kill_connection_errors = {
    metrics: {
      mssql_kill_connection_errors: new client.Gauge({
        name: "mssql_kill_connection_errors",
        help: "Number of kill connection errors/sec since last restart",
        labelNames: ["host"]
      })
    },
    query: `SELECT cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Errors/sec'
              AND instance_name = 'Kill Connection Errors'`,
    collect: (rows, metrics, host) => {
      const mssql_kill_connection_errors = rows[0][0];
      metricsLog("Fetched number of kill connection errors/sec", host, mssql_kill_connection_errors);
      metrics.mssql_kill_connection_errors.set({ host }, mssql_kill_connection_errors);
    }
  };

  const mssql_database_state = {
    metrics: {
      mssql_database_state: new client.Gauge({
        name: "mssql_database_state",
        help: "Databases states: 0=ONLINE 1=RESTORING 2=RECOVERING 3=RECOVERY_PENDING 4=SUSPECT 5=EMERGENCY 6=OFFLINE 7=COPYING 10=OFFLINE_SECONDARY",
        labelNames: ["host", "database"]
      })
    },
    query: `SELECT name, state
            FROM master.sys.databases`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const mssql_database_state = row[1];
        metricsLog("Fetched state for database", host, database, mssql_database_state);
        metrics.mssql_database_state.set({ host, database }, mssql_database_state);
      }
    }
  };

  const mssql_log_growths = {
    metrics: {
      mssql_log_growths: new client.Gauge({
        name: "mssql_log_growths",
        help: "Total number of times the transaction log for the database has been expanded last restart",
        labelNames: ["host", "database"]
      })
    },
    query: `SELECT rtrim(instance_name), cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Log Growths'
              and instance_name <> '_Total'`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const mssql_log_growths = row[1];
        metricsLog("Fetched number log growths for database", host, database, mssql_log_growths);
        metrics.mssql_log_growths.set({ host, database }, mssql_log_growths);
      }
    }
  };

  const mssql_database_filesize = {
    metrics: {
      mssql_database_filesize: new client.Gauge({
        name: "mssql_database_filesize",
        help: "Physical sizes of files used by database in KB, their names and types (0=rows, 1=log, 2=filestream,3=n/a 4=fulltext(before v2008 of MSSQL))",
        labelNames: ["host", "database", "logicalname", "type", "filename"]
      })
    },
    query: `SELECT DB_NAME(database_id) AS    database_name,
                   name                 AS    logical_name,
                   type,
                   physical_name,
                   (size * CAST(8 AS BIGINT)) size_kb
            FROM sys.master_files`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const logicalname = row[1];
        const type = row[2];
        const filename = row[3];
        const mssql_database_filesize = row[4];
        metricsLog(
          "Fetched size of files for database ",
          database,
          "host",
          host,
          "logicalname",
          logicalname,
          "type",
          type,
          "filename",
          filename,
          "size",
          mssql_database_filesize
        );
        metrics.mssql_database_filesize.set({
          host,
          database,
          logicalname,
          type,
          filename
        }, mssql_database_filesize);
      }
    }
  };

  const mssql_buffer_manager = {
    metrics: {
      mssql_page_read_total: new client.Gauge({
        name: "mssql_page_read_total",
        help: "Page reads/sec",
        labelNames: ["host"]
      }),
      mssql_page_write_total: new client.Gauge({
        name: "mssql_page_write_total",
        help: "Page writes/sec",
        labelNames: ["host"]
      }),
      mssql_page_life_expectancy: new client.Gauge({
        name: "mssql_page_life_expectancy",
        help: "Indicates the minimum number of seconds a page will stay in the buffer pool on this node without references. The traditional advice from Microsoft used to be that the PLE should remain above 300 seconds",
        labelNames: ["host"]
      }),
      mssql_lazy_write_total: new client.Gauge({
        name: "mssql_lazy_write_total",
        help: "Lazy writes/sec",
        labelNames: ["host"]
      }),
      mssql_page_checkpoint_total: new client.Gauge({
        name: "mssql_page_checkpoint_total",
        help: "Checkpoint pages/sec",
        labelNames: ["host"]
      })
    },
    query: `SELECT *
            FROM (SELECT rtrim(counter_name) as counter_name, cntr_value
                  FROM sys.dm_os_performance_counters
                  WHERE counter_name in
                        ('Page reads/sec', 'Page writes/sec', 'Page life expectancy', 'Lazy writes/sec',
                         'Checkpoint pages/sec')
                    AND object_name = 'SQLServer:Buffer Manager') d PIVOT
                     (
                     MAX(cntr_value)
                     FOR counter_name IN ([Page reads/sec], [Page writes/sec], [Page life expectancy], [Lazy writes/sec], [Checkpoint pages/sec])
                     ) piv
    `,
    collect: (rows, metrics, host) => {
      const row = rows[0];
      const page_read = row[0];
      const page_write = row[1];
      const page_life_expectancy = row[2];
      const lazy_write_total = row[3];
      const page_checkpoint_total = row[4];
      metricsLog(
        "Fetched the Buffer Manager",
        "host",
        host,
        "page_read",
        page_read,
        "page_write",
        page_write,
        "page_life_expectancy",
        page_life_expectancy,
        "page_checkpoint_total",
        "page_checkpoint_total",
        page_checkpoint_total,
        "lazy_write_total",
        lazy_write_total
      );
      metrics.mssql_page_read_total.set({ host }, page_read);
      metrics.mssql_page_write_total.set({ host }, page_write);
      metrics.mssql_page_life_expectancy.set({ host }, page_life_expectancy);
      metrics.mssql_page_checkpoint_total.set({ host }, page_checkpoint_total);
      metrics.mssql_lazy_write_total.set({ host }, lazy_write_total);
    }
  };

  const mssql_io_stall = {
    metrics: {
      mssql_io_stall: new client.Gauge({
        name: "mssql_io_stall",
        help: "Wait time (ms) of stall since last restart",
        labelNames: ["host", "database", "type"]
      }),
      mssql_io_stall_total: new client.Gauge({
        name: "mssql_io_stall_total",
        help: "Wait time (ms) of stall since last restart",
        labelNames: ["host", "database"]
      })
    },
    query: `SELECT cast(DB_Name(a.database_id) as varchar) as name,
                   max(io_stall_read_ms),
                   max(io_stall_write_ms),
                   max(io_stall),
                   max(io_stall_queued_read_ms),
                   max(io_stall_queued_write_ms)
            FROM sys.dm_io_virtual_file_stats(null, null) a
                     INNER JOIN sys.master_files b ON a.database_id = b.database_id and a.file_id = b.file_id
            GROUP BY a.database_id`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const read = row[1];
        const write = row[2];
        const stall = row[3];
        const queued_read = row[4];
        const queued_write = row[5];
        metricsLog("Fetched number of stalls for database", database, "host", host, "read", read, "write", write, "queued_read", queued_read, "queued_write", queued_write);
        metrics.mssql_io_stall_total.set({ host, database }, stall);
        metrics.mssql_io_stall.set({ host, database, type: "read" }, read);
        metrics.mssql_io_stall.set({ host, database, type: "write" }, write);
        metrics.mssql_io_stall.set({ host, database, type: "queued_read" }, queued_read);
        metrics.mssql_io_stall.set({ host, database, type: "queued_write" }, queued_write);
      }
    }
  };

  const mssql_batch_requests = {
    metrics: {
      mssql_batch_requests: new client.Gauge({
        name: "mssql_batch_requests",
        help: "Number of Transact-SQL command batches received per second. This statistic is affected by all constraints (such as I/O, number of users, cachesize, complexity of requests, and so on). High batch requests mean good throughput",
        labelNames: ["host"]
      })
    },
    query: `SELECT TOP 1 cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Batch Requests/sec'`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const mssql_batch_requests = row[0];
        metricsLog("Fetched number of batch requests per second", host, mssql_batch_requests);
        metrics.mssql_batch_requests.set({ host }, mssql_batch_requests);
      }
    }
  };

  const mssql_transactions = {
    metrics: {
      mssql_transactions: new client.Gauge({
        name: "mssql_transactions",
        help: "Number of transactions started for the database per second. Transactions/sec does not count XTP-only transactions (transactions started by a natively compiled stored procedure.)",
        labelNames: ["host", "database"]
      })
    },
    query: `SELECT rtrim(instance_name), cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Transactions/sec'
              AND instance_name <> '_Total'`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const transactions = row[1];
        metricsLog("Fetched number of transactions per second", database, transactions);
        metrics.mssql_transactions.set({ host, database }, transactions);
      }
    }
  };

  const mssql_os_process_memory = {
    metrics: {
      mssql_page_fault_count: new client.Gauge({
        name: "mssql_page_fault_count",
        help: "Number of page faults since last restart",
        labelNames: ["host"]
      }),
      mssql_memory_utilization_percentage: new client.Gauge({
        name: "mssql_memory_utilization_percentage",
        help: "Percentage of memory utilization",
        labelNames: ["host"]
      })
    },
    query: `SELECT page_fault_count, memory_utilization_percentage
            FROM sys.dm_os_process_memory`,
    collect: (rows, metrics, host) => {
      const page_fault_count = rows[0][0];
      const memory_utilization_percentage = rows[0][1];
      metricsLog("Fetched page fault count", page_fault_count);
      metrics.mssql_page_fault_count.set({ host }, page_fault_count);
      metrics.mssql_memory_utilization_percentage.set({ host }, memory_utilization_percentage);
    }
  };

  const mssql_os_sys_memory = {
    metrics: {
      mssql_total_physical_memory_kb: new client.Gauge({
        name: "mssql_total_physical_memory_kb",
        help: "Total physical memory in KB",
        labelNames: ["host"]
      }),
      mssql_available_physical_memory_kb: new client.Gauge({
        name: "mssql_available_physical_memory_kb",
        help: "Available physical memory in KB",
        labelNames: ["host"]
      }),
      mssql_total_page_file_kb: new client.Gauge({
        name: "mssql_total_page_file_kb",
        help: "Total page file in KB",
        labelNames: ["host"]
      }),
      mssql_available_page_file_kb: new client.Gauge({
        name: "mssql_available_page_file_kb",
        help: "Available page file in KB",
        labelNames: ["host"]
      })
    },
    query: `SELECT total_physical_memory_kb,
                   available_physical_memory_kb,
                   total_page_file_kb,
                   available_page_file_kb
            FROM sys.dm_os_sys_memory`,
    collect: (rows, metrics, host) => {
      const mssql_total_physical_memory_kb = rows[0][0];
      const mssql_available_physical_memory_kb = rows[0][1];
      const mssql_total_page_file_kb = rows[0][2];
      const mssql_available_page_file_kb = rows[0][3];
      metricsLog(
        "Fetched system memory information",
        "host",
        host,
        "Total physical memory",
        mssql_total_physical_memory_kb,
        "Available physical memory",
        mssql_available_physical_memory_kb,
        "Total page file",
        mssql_total_page_file_kb,
        "Available page file",
        mssql_available_page_file_kb
      );
      metrics.mssql_total_physical_memory_kb.set({ host }, mssql_total_physical_memory_kb);
      metrics.mssql_available_physical_memory_kb.set({ host }, mssql_available_physical_memory_kb);
      metrics.mssql_total_page_file_kb.set({ host }, mssql_total_page_file_kb);
      metrics.mssql_available_page_file_kb.set({ host }, mssql_available_page_file_kb);
    }
  };


  const mssql_db_memory = {
    metrics: {
      mssql_db_memory: new client.Gauge({
        name: "mssql_db_memory",
        help: "RAM used by database",
        labelNames: ["host", "database"]
      })
    },
    query: `SELECT ISNULL(DB_NAME(database_id), 'null') As [database_name],
                   COUNT(1) * 8                         AS memory_usage
            FROM sys.dm_os_buffer_descriptors
            GROUP BY database_id
            ORDER BY COUNT(*) DESC`,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const memory_usage = row[1];
        metricsLog("RAM per database", database, "host", host, "memory_usage", memory_usage);
        metrics.mssql_db_memory.set({ host, database }, memory_usage);
      }
    }
  };

  const mssql_volume_stats = {
    metrics: {
      mssql_volume_total_bytes: new client.Gauge({
        name: "mssql_volume_total_bytes",
        help: "Total size in bytes of the volume",
        labelNames: ["host", "volume_mount_point"]
      }),
      mssql_volume_available_bytes: new client.Gauge({
        name: "mssql_volume_available_bytes",
        help: "Available free space on the volume",
        labelNames: ["host", "volume_mount_point"]
      })
    },
    query: `
        SELECT distinct(volume_mount_point), total_bytes, avg(available_bytes) as available_bytes
        FROM sys.master_files AS f
                 CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id)
        GROUP by volume_mount_point, total_bytes
    `,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const volume_mount_point = row[0].value;
        const total_bytes = +row[1].value;
        const available_bytes = +row[2].value;
        debug("Fetch volume stats for volume_mount_point ", volume_mount_point);
        metrics.mssql_volume_total_bytes.set(
          { host, volume_mount_point },
          total_bytes
        );
        metrics.mssql_volume_available_bytes.set(
          { host, volume_mount_point },
          available_bytes
        );
      }
    }
  };

  return {
    mssql_up,
    mssql_product_version,
    mssql_instance_local_time,
    mssql_connections,
    mssql_client_connections,
    mssql_deadlocks,
    mssql_user_errors,
    mssql_kill_connection_errors,
    mssql_database_state,
    mssql_log_growths,
    mssql_database_filesize,
    mssql_buffer_manager,
    mssql_io_stall,
    mssql_batch_requests,
    mssql_transactions,
    mssql_os_process_memory,
    mssql_os_sys_memory,
    mssql_db_memory,
    mssql_volume_stats
  };
}

module.exports = {
  getMetrics
};
