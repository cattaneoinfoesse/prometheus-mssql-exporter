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
                   sum(io_stall_read_ms) as io_stall_read_ms,
                   sum(io_stall_write_ms) as io_stall_write_ms,
                   sum(io_stall) as io_stall,
                   sum(io_stall_queued_read_ms) as io_stall_queued_read_ms,
                   sum(io_stall_queued_write_ms) as io_stall_queued_write_ms,
                   sum(io_stall_read_ms) / sum(num_of_reads) as io_stall_read_ms_avg,
                   sum(io_stall_write_ms) / sum(num_of_writes) as io_stall_write_ms_avg
            FROM sys.dm_io_virtual_file_stats(null, null) a
                     INNER JOIN sys.master_files b ON a.database_id = b.database_id AND a.file_id = b.file_id
            WHERE b.[type] = 0 /* only rows file */
            GROUP BY a.database_id
    `,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const database = row[0];
        const read = row[1];
        const write = row[2];
        const stall = row[3];
        const queued_read = row[4];
        const queued_write = row[5];
        const avg_read = row[6];
        const avg_write = row[7];
        metricsLog("Fetched number of stalls for database", database, "host", host, "read", read, "write", write, "queued_read", queued_read, "queued_write", queued_write, "avg_read", avg_read, "avg_write", avg_write);
        metrics.mssql_io_stall_total.set({ host, database }, stall);
        metrics.mssql_io_stall.set({ host, database, type: "read" }, read);
        metrics.mssql_io_stall.set({ host, database, type: "write" }, write);
        metrics.mssql_io_stall.set({ host, database, type: "queued_read" }, queued_read);
        metrics.mssql_io_stall.set({ host, database, type: "queued_write" }, queued_write);
        metrics.mssql_io_stall.set({ host, database, type: "avg_read" }, avg_read);
        metrics.mssql_io_stall.set({ host, database, type: "avg_write" }, avg_write);
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
      }),
      mssql_volume_available_percentage: new client.Gauge({
        name: "mssql_volume_available_percentage",
        help: "Available free space on the volume ( % )",
        labelNames: ["host", "volume_mount_point"]
      })
    },
    query: `
        SELECT distinct(volume_mount_point),
                       total_bytes,
                       avg(available_bytes),
                       cast(avg(available_bytes) as decimal) / cast(total_bytes as decimal) * 100
        FROM sys.master_files AS f
                 CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id)
        GROUP by volume_mount_point, total_bytes
    `,
    collect: (rows, metrics, host) => {
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const volume_mount_point = row[0];
        const total_bytes = +row[1];
        const available_bytes = +row[2];
        const available_percentage = +row[3];
        metricsLog("Fetch volume stats for volume_mount_point ", volume_mount_point);
        metrics.mssql_volume_total_bytes.set(
          { host, volume_mount_point },
          total_bytes
        );
        metrics.mssql_volume_available_bytes.set(
          { host, volume_mount_point },
          available_bytes
        );
        metrics.mssql_volume_available_percentage.set(
          { host, volume_mount_point },
          available_percentage
        );
      }
    }
  };

  const mssql_most_exec_query = {
    metrics: {
      mssql_total_execution_count: new client.Gauge({name: 'mssql_total_execution_count', help: 'Total Execution Count', labelNames: ["database", "query_id", "query_text_id", "query_sql_text"]}),
    },
    query: `
        SELECT TOP 100 q.query_id, qt.query_text_id, qt.query_sql_text, SUM(rs.count_executions) AS total_execution_count
        FROM sys.query_store_query_text AS qt
                 JOIN sys.query_store_query AS q
                      ON qt.query_text_id = q.query_text_id
                 JOIN sys.query_store_plan AS p
                      ON q.query_id = p.query_id
                 JOIN sys.query_store_runtime_stats AS rs
                      ON p.plan_id = rs.plan_id
        WHERE rs.avg_duration > 1000000
        GROUP BY q.query_id, qt.query_text_id, qt.query_sql_text
        ORDER BY total_execution_count DESC
    `,
    collect: function (rows, metrics, config, host) {
      let dbname = config.connect.options.database;
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const mssql_query_id = row[0];
        const mssql_query_text_id = row[1];
        const mssql_query_sql_text = row[2];
        const mssql_total_execution_count = row[3];
        metricsLog("Most Executed Queries -", dbname);
        metrics.mssql_total_execution_count.set({host, database: dbname, query_id: mssql_query_id, query_text_id: mssql_query_text_id, query_sql_text: mssql_query_sql_text},mssql_total_execution_count);
      }
    }

  };

  const mssql_most_avg_time_query = {
    metrics: {
      mssql_avg_duration: new client.Gauge({name: 'mssql_avg_duration_us', help: 'Average Query Duration in micro seconds', labelNames: ["database", "query_sql_text", "query_id"]}),
    },
    query: `SELECT TOP 100 avg(rs.avg_duration) AS avg_duration, max(qt.query_sql_text) AS query_sql_text, q.query_id, GETUTCDATE() AS CurrentUTCTime, max(rs.last_execution_time) AS last_execution_time
FROM sys.query_store_query_text AS qt       
JOIN sys.query_store_query AS q          
    ON qt.query_text_id = q.query_text_id              
JOIN sys.query_store_plan AS p           
    ON q.query_id = p.query_id        
JOIN sys.query_store_runtime_stats AS rs          
    ON p.plan_id = rs.plan_id              
WHERE rs.last_execution_time > DATEADD(hour, -1, GETUTCDATE()) and rs.avg_duration > 1000000
GROUP BY q.query_id      
ORDER BY avg(rs.avg_duration) DESC`,
    collect: function (rows, metrics, config, host) {
      let dbname = config.connect.options.database;
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const mssql_avg_duration = row[0];
        const mssql_query_sql_text = row[1];
        const mssql_query_id = row[2];
        metricsLog("Most Average Time Query -", dbname);
        metrics.mssql_avg_duration.set({host, database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_avg_duration);
      }
    }
  };

  const mssql_most_avg_io_query = {
    metrics: {
      mssql_avg_physical_io_reads: new client.Gauge({name: 'mssql_avg_physical_io_reads', help: 'Average Physical IO Reads', labelNames: ["database", "query_sql_text", "query_id"]}),
      mssql_avg_rowcount : new client.Gauge({name: 'mssql_avg_rowcount', help: 'Average Row Count', labelNames: ["database", "query_sql_text", "query_id"]}),
      mssql_count_executions : new client.Gauge({name: 'mssql_count_executions', help: 'Cont Executions', labelNames: ["database", "query_sql_text", "query_id"]}),
    },
    query: `SELECT TOP 10 avg(rs.avg_physical_io_reads) as avg_physical_io_reads, max(qt.query_sql_text) as query_sql_text, q.query_id, avg(rs.avg_rowcount) as avg_rowcount, sum(rs.count_executions) as count_executions
FROM sys.query_store_query_text AS qt
JOIN sys.query_store_query AS q
    ON qt.query_text_id = q.query_text_id
JOIN sys.query_store_plan AS p
    ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats AS rs
    ON p.plan_id = rs.plan_id
JOIN sys.query_store_runtime_stats_interval AS rsi 
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(hour, -1, GETUTCDATE()) and rs.avg_duration > 1000000
GROUP BY q.query_id
ORDER BY avg(rs.avg_physical_io_reads) DESC`,
    collect: function (rows, metrics, config, host) {
      let dbname = config.connect.options.database;
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const mssql_avg_physical_io_reads = row[0];
        const mssql_query_sql_text = row[1];
        const mssql_query_id = row[2];
        const mssql_avg_rowcount = row[3];
        const mssql_count_executions = row[4];
        metricsLog("Most Average IO Query -", dbname);
        metrics.mssql_avg_physical_io_reads.set({host, database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_avg_physical_io_reads);
        metrics.mssql_avg_rowcount.set({host, database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_avg_rowcount);
        metrics.mssql_count_executions.set({host, database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_count_executions);
      }
    }
  };

  const mssql_most_wait_query = {
    metrics: {
      mssql_sum_total_wait_ms: new client.Gauge({name: 'mssql_sum_total_wait_ms', help: 'Total Wait ms', labelNames: ["database", "query_sql_text", "query_text_id", "query_id"]}),
    },
    query: `SELECT qt.query_sql_text, qt.query_text_id, st.sum_total_wait_ms,  q.query_id
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN (
 SELECT TOP 50  p.query_id, sum(total_query_wait_time_ms) AS sum_total_wait_ms 
 FROM sys.query_store_wait_stats ws 
 JOIN sys.query_store_plan p ON ws.plan_id = p.plan_id 
 JOIN sys.query_store_query q ON p.query_id = q.query_id 
 JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id 
 JOIN sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id 
 WHERE rs.avg_duration > 1000000 
 GROUP BY p.query_id 
 ORDER BY sum_total_wait_ms DESC) as st ON st.query_id = q.query_id`,
    collect: function (rows, metrics, config, host) {
      let dbname = config.connect.options.database;
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const mssql_query_sql_text = row[0];
        const mssql_query_text_id = row[1];
        const mssql_sum_total_wait_ms = row[2];
        const mssql_query_id = row[3];
        metricsLog("Most Wait Query -", dbname);
        metrics.mssql_sum_total_wait_ms.set({host, database: dbname, query_sql_text: mssql_query_sql_text, query_text_id: mssql_query_text_id, query_id: mssql_query_id},mssql_sum_total_wait_ms);
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
    mssql_volume_stats,
    mssql_most_exec_query,
    mssql_most_avg_io_query,
    mssql_most_avg_time_query
  };
}

module.exports = {
  getMetrics
};
