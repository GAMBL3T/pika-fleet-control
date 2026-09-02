'use strict'

// The dashboard is intentionally separate from worker.js. Restarting this
// process only restarts the control UI; connected Mineflayer clients stay in
// the persistent worker process.
require('./dashboard')
