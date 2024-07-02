const {
  updateProject,
  updateCalendars,
  updateTasks,
  updateResources,
  updateDependencies,
  createAssignments,
} = require("./index.js");

require("dotenv").config();
const neo4j = require("neo4j-driver");
const fs = require("fs");

const URI = process.env.NEO4J_URI;
const USER = process.env.NEO4J_USERNAME;
const PASSWORD = process.env.NEO4J_PASSWORD;
const DBNAME = process.env.NEO4J_DBNAME || "neo4j";

let driver;

(async () => {
  try {
    driver = neo4j.driver(URI, neo4j.auth.basic(USER, PASSWORD));
    const serverInfo = await driver.getServerInfo();
    console.log("Connection established");
    console.log(serverInfo);
  } catch (err) {
    console.log(`Connection error\n${err}\nCause: ${err.cause}`);
  }
})();

(async () => {
  console.log("Checking schema");
  let schema_statements = [
    "create constraint if not exists for (n:Task) require (n.id) is node key",
    "create constraint if not exists for (n:Resource) require (n.id) is node key",
    "create constraint if not exists for (n:Calendar) require (n.id) is node key",
    "create constraint if not exists for (n:Resource) require (n.id) is node key",
    "create constraint if not exists for ()-[n:ASSIGNED_TO]-() require (n.id) is relationship key",
    "create constraint if not exists for (n:Interval) require (n.id,n.recurrentStartDate,n.recurrentEndDate) is node key",
  ];

  for (let i = 0; i < schema_statements.length; i++) {
    await driver.executeQuery(
      schema_statements[i],
      {},
      { database: DBNAME },
      { routing: neo4j.routing.WRITE }
    );
  }
})();

(async () => {
  console.log("Checking initial data");
  data = JSON.parse(fs.readFileSync("data.json", {}));

  const session = driver.session({ database: DBNAME });
  try {
    await session.writeTransaction(async (txc) => {
      return await Promise.all([
        await updateProject(txc, data.project),
        await updateCalendars(txc, data.calendars.rows),
        await updateTasks(txc, data.tasks.rows),
        await updateResources(txc, data.resources.rows),
        await updateDependencies(txc, data.dependencies.rows),
        await createAssignments(txc, data.assignments.rows),
      ]);
    });
    console.log("Data migration success!");
  } finally {
    session.close();
  }
})();
