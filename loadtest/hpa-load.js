// k6 sustained load test (requirement c): drives the HPAs of incident-intake and quorum-coordinator.
// Run with scripts/load-test.sh (uses the official k6 Docker image, nothing to install).
// Mix: 70 % report an incident (quorum write W=2), 30 % read one back (quorum read R=2).
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL;
const PEAK = parseInt(__ENV.PEAK_VUS || '40');
const HOLD = __ENV.HOLD || '6m';
const CRIT_PCT = parseFloat(__ENV.CRIT_PCT || '1');   // % CRITICAL (each triggers auto-dispatch + alerts)

export const options = {
  stages: [
    { duration: '1m', target: Math.ceil(PEAK / 2) },  // warm up
    { duration: '1m', target: PEAK },                 // ramp to peak
    { duration: HOLD, target: PEAK },                 // SUSTAINED traffic -> HPA scales out
    { duration: '1m', target: 0 },                    // ramp down -> HPA scales back in afterwards
  ],
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<2000'],
  },
};

const REGIONS = ['north', 'south', 'east', 'west', 'central'];
const TYPES = ['FLOOD', 'FIRE', 'EARTHQUAKE', 'CYCLONE', 'LANDSLIDE', 'MEDICAL', 'OTHER'];
const ids = [];

function severity() {
  const r = Math.random() * 100;
  if (r < CRIT_PCT) return 'CRITICAL';
  if (r < 20) return 'HIGH';
  if (r < 60) return 'MEDIUM';
  return 'LOW';
}

export default function () {
  if (ids.length === 0 || Math.random() < 0.7) {
    const res = http.post(`${BASE}/api/incidents`, JSON.stringify({
      reporterId: `k6-${__VU}-${Math.floor(Math.random() * 20)}`,
      type: TYPES[Math.floor(Math.random() * TYPES.length)],
      severity: severity(),
      region: REGIONS[Math.floor(Math.random() * REGIONS.length)],
      description: `load test report VU ${__VU} iteration ${__ITER}`,
      latitude: 8 + Math.random() * 25,
      longitude: 68 + Math.random() * 25,
      peopleAffected: Math.floor(Math.random() * 300),
    }), { headers: { 'Content-Type': 'application/json' }, tags: { op: 'write' } });
    if (check(res, { 'write 201': (r) => r.status === 201 }) && ids.length < 100) {
      ids.push(res.json('incidentId'));
    }
  } else {
    const id = ids[Math.floor(Math.random() * ids.length)];
    const res = http.get(`${BASE}/api/incidents/${id}`, { tags: { op: 'read' } });
    check(res, { 'read 200': (r) => r.status === 200 });
  }
  sleep(0.2 + Math.random() * 0.3);
}
