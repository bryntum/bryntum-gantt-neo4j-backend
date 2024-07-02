const express = require("express");
const app = express();
const port = 3000;
require("dotenv").config();
const neo4j = require("neo4j-driver");

app.use(express.json());

const URI = process.env.NEO4J_URI;
const USER = process.env.NEO4J_USERNAME;
const PASSWORD = process.env.NEO4J_PASSWORD;
const DBNAME = process.env.NEO4J_DBNAME || "neo4j";

app.get("/load", (req, res) => {
  res.send("Hello World!");
});

app.post("/sync", (req, res) => {
  res.send("Hello World!");
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
