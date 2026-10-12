#include <doctest.h>

#include "flashtick/api_server.h"

using namespace flashtick;

TEST_CASE("ApiServer parses request line, headers, query and body") {
    ApiServer server;
    auto req = server.parse_request(
        "GET /metrics?format=text&x=1 HTTP/1.1\r\nHost: localhost\r\nAccept: */*\r\n\r\n");
    CHECK(req.method == "GET");
    CHECK(req.path == "/metrics");
    CHECK(req.query_params["format"] == "text");
    CHECK(req.query_params["x"] == "1");
    CHECK(req.headers["Host"] == "localhost");

    auto post = server.parse_request("POST /reload HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}");
    CHECK(post.method == "POST");
    CHECK(post.body == "{}");
}

TEST_CASE("ApiServer routes health without an agent and 404s unknown paths") {
    ApiServer server;
    HttpRequest req;
    req.method = "GET";
    req.path = "/health/";
    auto res = server.route_request(req);
    CHECK(res.status_code == 200);
    CHECK(res.body == "{\"status\":\"ok\"}");

    req.path = "/nope";
    CHECK(server.route_request(req).status_code == 404);

    req.path = "/status";
    CHECK(server.route_request(req).status_code == 500); // no agent attached
}
