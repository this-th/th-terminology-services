// Variables: https://nginx.org/en/docs/njs/reference.html
async function authz(r) {
    if (r.headersIn["Authorization"] === undefined) {
        r.error("Bearer undefined");
        r.return(401);
        return;
    }

    if (!r.headersIn["Authorization"].includes("Bearer")) {
        r.error("No Bearer");
        r.return(401);
        return;
    }

    if (r.headersIn["Authorization"].split(' ').length < 2) {
        r.error("Incorrect Bearer format");
        r.return(401);
        return;
    }

    const opa_data = {
        "input": {
            "token": r.headersIn["Authorization"].split(' ')[1],
            "path": r.variables.request_uri.split('?')[0],
            "method": r.variables.request_method
        }
    };

    r.error("OPA input: " + JSON.stringify(opa_data));

    const opts = {
        method: "POST",
        body: JSON.stringify(opa_data)
    }

    r.subrequest("/_opa", opts, function(opa) {

        const body = JSON.parse(opa.responseBody);
        if (!body.result)  {
            r.return(401);
            return;
        }

        if (!body.result.allow) {
            r.return(401);
            return;
        } else {
            r.return(200);
        }
    });
}

export default { authz }
