const express = require('express');
const path = require('path');

const app = express();
const port = 3000;

app.use(express.json());
app.use(express.static('public'));

/**
 * Starts the server and initializes database connection
 * @function start_server
 * @returns {void}
 */
async function start_server() {
  try {
    app.listen(port, () => {
      console.log(`Shift management app running at http://localhost:${port}`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
  }
}

start_server();