defmodule ReqSnowflake.SnowflakeTestHelpers do
  alias Plug.Conn

  def valid_login_bypass(bypass) do
    Bypass.expect(bypass, "POST", "/session/v1/login-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/snowflake_valid_login_response.json"
        ])
      )
      |> json(conn, 200)
    end)
  end

  def query_bypass(bypass, query_file) do
    Bypass.expect(bypass, "POST", "/queries/v1/query-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          query_file
        ])
      )
      |> json(conn, 200)
    end)
  end

  def query_bypass_chunks(bypass, query_file, chunks) do
    Bypass.expect(bypass, "POST", "/queries/v1/query-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          query_file
        ])
      )
      |> Jason.decode!()
      |> put_in(["data", "chunks"], chunks)
      |> Jason.encode!()
      |> json(conn, 200)
    end)
  end

  def s3_bypass(bypass, location, file) do
    Bypass.expect(bypass, "GET", "/#{location}", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          file
        ])
      )
      |> json_gzip(conn, 200)
    end)
  end

  # Snowflake sends back gzipped encoding, so gzip the response here we send back with bypass.
  # Also send back some other headers s3 sends for goodluck.
  defp json_gzip(data, conn, status) do
    conn
    |> Conn.put_resp_content_type("binary/octet-stream")
    |> Conn.put_resp_header("content-encoding", "gzip")
    |> Conn.put_resp_header("x-amz-id-2", "aaa")
    |> Conn.put_resp_header("x-amz-request-id", "xxx")
    |> Conn.put_resp_header("x-amz-server-side-encryption-customer-algorithm", "AES256")
    |> Conn.put_resp_header("x-amz-server-side-encryption-customer-key-md5", "abcd")
    |> Conn.put_resp_header("accept-ranges", "bytes")
    |> Conn.send_resp(status, :zlib.gzip(data))
  end

  defp json(data, conn, status) do
    conn
    |> Conn.put_resp_content_type("application/json")
    |> Conn.send_resp(status, data)
  end
end
