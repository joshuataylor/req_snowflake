defmodule ReqSnowflake.SnowflakeCase do
  use ExUnit.CaseTemplate

  setup do
    bypass = Bypass.open()
    valid_login_bypass(bypass)

    Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
    Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:#{bypass.port}")
    Application.put_env(:req_snowflake, :snowflake_uuid, "0000000-0000-0000-0000-000000000000")

    {:ok, %{bypass: bypass}}
  end

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

  defp json(data, conn, status) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, data)
  end
end
