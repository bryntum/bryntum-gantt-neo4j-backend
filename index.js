const express = require("express");
const app = express();
const port = 3000;
require("dotenv").config();
const neo4j = require("neo4j-driver");

const {
  getProject,
  getCalendars,
  getAssignments,
  getResources,
  getTasks,
  getDependecies,
  id_util,
  syncTasks,
  syncAssignments,
  syncDependencies,
  syncResources,
} = require("./helper");

app.use(express.json());

const URI = process.env.NEO4J_URI;
const USER = process.env.NEO4J_USERNAME;
const PASSWORD = process.env.NEO4J_PASSWORD;
const DBNAME = process.env.NEO4J_DBNAME || "neo4j";

(async () => {
  try {
    driver = neo4j.driver(URI, neo4j.auth.basic(USER, PASSWORD));
    const serverInfo = await driver.getServerInfo();
    console.log("Connection established");
    console.log(serverInfo);
  } catch (err) {
    console.log(`Connection error\\n${err}\\nCause: ${err.cause}`);
  }
})();

app.get("/load", async (req, res) => {
  const session = driver.session({ database: DBNAME });
  try {
    const [tasks, dependencies, resources, assignments, calendars, project] =
      await session.readTransaction(async (txc) => {
        return await Promise.all([
          getTasks(txc),
          getDependecies(txc),
          getResources(txc),
          getAssignments(txc),
          getCalendars(txc),
          getProject(txc),
        ]);
      });
    res.send({
      success: true,
      project: project,
      calendars: {
        rows: calendars,
      },
      tasks: {
        rows: tasks,
      },
      dependencies: {
        rows: dependencies,
      },
      resources: {
        rows: resources,
      },
      assignments: {
        rows: assignments,
      },
    });
  } catch (error) {
    res.send({
      success: false,
      // pass raw exception message to the client as-is
      // please replace with something more human-readable before using this on production systems
      message: error.message,
    });
  } finally {
    await session.close();
  }
});

app.post("/sync", async (req, res) => {
  const { requestId, tasks, resources, assignments, dependencies } = id_util(
    req.body
  );
  const session = driver.session({ database: DBNAME });
  try {
    const [tasks_res, resources_res, assignments_res, dependencies_res] =
      await session.writeTransaction(async (txc) => {
        return await Promise.all([
          syncTasks(txc, tasks),
          syncResources(txc, resources),
          syncAssignments(txc, assignments),
          syncDependencies(txc, dependencies),
        ]);
      });
    const response = { requestId, success: true };
    // if task changes are passed
    if (tasks) {
      const rows = tasks_res;
      // if got some new data to update client
      if (rows) {
        response.tasks = { rows };
      }
    }
    if (resources) {
      const rows = resources_res;
      // if got some new data to update client
      if (rows) {
        response.resources = { rows };
      }
    }
    if (assignments) {
      const rows = assignments_res;
      // if got some new data to update client
      if (rows) {
        response.assignments = { rows };
      }
    }
    if (dependencies) {
      const rows = dependencies_res;
      // if got some new data to update client
      if (rows) {
        response.dependencies = { rows };
      }
    }
    res.send(response);
  } catch (error) {
    console.log(error.message);
    res.send({
      requestId,
      success: false,
      // pass raw exception message to the client as-is
      // please replace with something more human readable before using this on production systems
      message: error.message,
    });
  } finally {
    await session.close();
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
