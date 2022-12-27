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

jwt_payload = payload if {
	[_, payload, _] := io.jwt.decode(input.token)
}

jwt_verified if {
	jwks := json.marshal(data.jwks)
	now := time.now_ns()
	# Check signature
	io.jwt.verify_rs256(input.token, jwks)
	# Check expire time
	jwt_payload.exp * 1000 * 1000 * 1000 > now
	# Check audience
	"https://fhirterm.sil-th.org" == jwt_payload.aud[i]
}

user_roles = jwt_payload.realm_access.roles

roles_allow if {
	user_allow_paths = data.roles[user_roles[_]]
	user_rules = user_allow_paths[input.path]
	user_rules[_] = input.method
}

is_admin if {
	"TerminologyAdmin" == user_roles[i]
}
