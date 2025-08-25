import http from "k6/http";
import { check } from "k6";

export const options = {
    scenarios: {
        rps: {
            executor: "constant-arrival-rate",
            rate: 1000,
            timeUnit: "1s",
            duration: "2m",
            preAllocatedVUs: 200,
            maxVUs: 1000,
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<200"],
    },
};

export default function () {
    const payload = JSON.stringify({
        payload: { kind: "arrow", power: 1 }
    });
    const res = http.post("http://host.docker.internal:8080/api/v1/emit", payload, {
        headers: { "Content-Type": "application/json" },
    });
    check(res, { "202 accepted": (r) => r.status === 202 });
}