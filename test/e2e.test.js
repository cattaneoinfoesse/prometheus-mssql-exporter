const request = require("superagent");

function parse(text) {
  let lines = text.split("\n");
  lines = lines.filter((line) => !line.startsWith("#")).filter((line) => line.length !== 0);
  const o = {};
  lines.forEach((line) => {
    expect(line.indexOf(" ")).toBeGreaterThanOrEqual(0);
    [key, value] = line.split(" ");
    o[key] = parseInt(value);
  });
  return o;
}

describe("E2E Test", function () {
  it("Fetch all metrics and ensure that all expected are present", async function () {
    const data = await request.get("http://localhost:4000/metrics");
    expect(data.status).toBe(200);
    let text = data.text;
    const lines = parse(text);
    const host = "localhost";
    // some specific tests
    expect(lines[`mssql_up{host="${host}"}`]).toBe(1);
    expect([14, 15]).toContain(lines[`mssql_product_version{host="${host}"}`]);
    expect(lines[`mssql_instance_local_time{host="${host}"}`]).toBeGreaterThan(0);
    expect(lines[`mssql_total_physical_memory_kb{host="${host}"}`]).toBeGreaterThan(0);

    // lets ensure that there is at least one instance of these 2019 entries (that differ from 2017)
    const v2019 = [`mssql_client_connections`, `mssql_database_filesize`];
    v2019.forEach((k2019) => {
      const keys = Object.keys(lines);
      const i = keys.findIndex((key) => key.startsWith(k2019));
      expect(i).toBeGreaterThanOrEqual(0);
      keys
        .filter((key) => key.startsWith(k2019))
        .forEach((key) => {
          delete lines[key];
        });
    });

    // bulk ensure that all expected results of a vanilla mssql server instance are here
    expect(Object.keys(lines).sort()).toEqual([
      `mssql_up{host="${host}"}`,
      `mssql_product_version{host="${host}"}`,
      `mssql_instance_local_time{host="${host}"}`,
      `mssql_connections{host="${host}",database="master",state="current"}`,
      `mssql_deadlocks{host="${host}"}`,
      `mssql_user_errors{host="${host}"}`,
      `mssql_kill_connection_errors{host="${host}"}`,
      `mssql_database_state{host="${host}",database="master"}`,
      `mssql_database_state{host="${host}",database="tempdb"}`,
      `mssql_database_state{host="${host}",database="model"}`,
      `mssql_database_state{host="${host}",database="msdb"}`,
      `mssql_log_growths{host="${host}",database="tempdb"}`,
      `mssql_log_growths{host="${host}",database="model"}`,
      `mssql_log_growths{host="${host}",database="msdb"}`,
      `mssql_log_growths{host="${host}",database="mssqlsystemresource"}`,
      `mssql_log_growths{host="${host}",database="master"}`,
      `mssql_page_read_total{host="${host}"}`,
      `mssql_page_write_total{host="${host}"}`,
      `mssql_page_life_expectancy{host="${host}"}`,
      `mssql_lazy_write_total{host="${host}"}`,
      `mssql_page_checkpoint_total{host="${host}"}`,
      `mssql_io_stall{host="${host}",database="master",type="read"}`,
      `mssql_io_stall{host="${host}",database="master",type="write"}`,
      `mssql_io_stall{host="${host}",database="master",type="queued_read"}`,
      `mssql_io_stall{host="${host}",database="master",type="queued_write"}`,
      `mssql_io_stall{host="${host}",database="tempdb",type="read"}`,
      `mssql_io_stall{host="${host}",database="tempdb",type="write"}`,
      `mssql_io_stall{host="${host}",database="tempdb",type="queued_read"}`,
      `mssql_io_stall{host="${host}",database="tempdb",type="queued_write"}`,
      `mssql_io_stall{host="${host}",database="model",type="read"}`,
      `mssql_io_stall{host="${host}",database="model",type="write"}`,
      `mssql_io_stall{host="${host}",database="model",type="queued_read"}`,
      `mssql_io_stall{host="${host}",database="model",type="queued_write"}`,
      `mssql_io_stall{host="${host}",database="msdb",type="read"}`,
      `mssql_io_stall{host="${host}",database="msdb",type="write"}`,
      `mssql_io_stall{host="${host}",database="msdb",type="queued_read"}`,
      `mssql_io_stall{host="${host}",database="msdb",type="queued_write"}`,
      `mssql_io_stall_total{host="${host}",database="master"}`,
      `mssql_io_stall_total{host="${host}",database="tempdb"}`,
      `mssql_io_stall_total{host="${host}",database="model"}`,
      `mssql_io_stall_total{host="${host}",database="msdb"}`,
      `mssql_batch_requests{host="${host}"}`,
      `mssql_transactions{host="${host}",database="tempdb"}`,
      `mssql_transactions{host="${host}",database="model"}`,
      `mssql_transactions{host="${host}",database="msdb"}`,
      `mssql_transactions{host="${host}",database="mssqlsystemresource"}`,
      `mssql_transactions{host="${host}",database="master"}`,
      `mssql_page_fault_count{host="${host}"}`,
      `mssql_memory_utilization_percentage{host="${host}"}`,
      `mssql_total_physical_memory_kb{host="${host}"}`,
      `mssql_available_physical_memory_kb{host="${host}"}`,
      `mssql_total_page_file_kb{host="${host}"}`,
      `mssql_available_page_file_kb{host="${host}"}`,
      `mssql_db_memory{host="${host}",database="null"}`,
      `mssql_db_memory{host="${host}",database="msdb"}`,
      `mssql_db_memory{host="${host}",database="master"}`,
      `mssql_db_memory{host="${host}",database="tempdb"}`,
      `mssql_db_memory{host="${host}",database="model"}`,
      `mssql_volume_total_bytes{host="${host}",volume_mount_point="null"}`,
      `mssql_volume_available_bytes{host="${host}",volume_mount_point="null"}`,
    ].sort());
  });
});
