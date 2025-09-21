import http from 'k6/http';
import { sleep } from 'k6';

export let options = {
  vus: 10,
  duration: '2m',
};

export default function () {
  http.get('http://demo-flask.default.svc.cluster.local/?load=0.05');
  sleep(1);
}
