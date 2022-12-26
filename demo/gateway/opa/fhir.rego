# Input example
# {
#   "token": "<bearer_token>",
#   "path" : "/fhir-server/api/v4/CodeSystem/$lookup",
#   "method" "GET"
# }

package terminology.fhir
import future.keywords.if

default allow := false

allow if {
	jwt_verified
    roles_allow
}

allow if {
	jwt_verified
    is_admin
}

jwt_verified {
    jwks := json.marshal(data.jwks)
	io.jwt.verify_rs256(input.token, jwks)
}

user_roles = payload.realm_access.roles {
	[_, payload, _] := io.jwt.decode(input.token)
}

roles_allow {
	user_allow_paths = data.roles[user_roles[_]]
    user_rules = user_allow_paths[input.path]
    user_rules[_] = input.method
}

is_admin {
	"TerminologyAdmin" == user_roles[i]
}
