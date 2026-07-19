import http from 'k6/http';

export const options = {
  scenarios: {
    egress_rate_limit_test: {
      executor: 'constant-arrival-rate',
      rate: 25,               // 25 requests per second
      timeUnit: '1s',         
      duration: '30s',        // Run for 30 seconds (25 * 30 = 750 total requests)
      preAllocatedVUs: 5,    
      maxVUs: 20,            
    },
  },
};

export default function () {
  const url = 'http://localhost:8888/notifications';
  
  // Distribute requests evenly across exactly 5 users
  const userId = Math.floor(Math.random() * 5) + 1; 
  
  const payload = JSON.stringify({
    userId: `user_${userId}`,
    type: 'EMAIL',
    message: 'Testing egress sliding window rate limits!'
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  http.post(url, payload, params);
}