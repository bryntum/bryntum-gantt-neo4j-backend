const express = require("express");
const cors = require("cors");
require("dotenv").config();
const neo4j = require("neo4j-driver");
const app = express();
const port = 3000;

app.use(express.json());
app.use(cors());

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
    console.log(`Connection error\n${err}\nCause: ${err.cause}`);
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

function buildTreeArray(flatArray) {
  const nodeMap = {};
  const result = [];
  flatArray.forEach((item) => {
    nodeMap[item.id] = { ...item, children: [] };
  });
  flatArray.forEach((item) => {
    const node = nodeMap[item.id];
    if (item.parentId !== null) {
      nodeMap[item.parentId].children.push(node);
    } else {
      result.push(node);
    }
  });
  return result;
}

async function getTasks(txc) {
  var result = await txc.run(`
      match (n:Task)
      return collect(apoc.map.merge( n{.*}, {
        parentId: [ (n)-[:PARENT_TASK]->(p) | p.id ][0],
        baselines: [ (n)-[:HAS_BASELINE]->(b) | b{.*} ]
      } )) as tasks
      `);
  response = result.records.map((record) => record.get("tasks"))[0];
  tasks = buildTreeArray(response);
  return tasks;
}

async function getDependecies(txc) {
  var result = await txc.run(`
        match (f:Task)-[d:DEPENDS_ON]->(t:Task)
        return collect( 
            apoc.map.merge( d{.*}, 
            {from:f.id, to:t.id})
        ) as dependencies
      `);
  return result.records.map((record) => record.get("dependencies"))[0];
}

async function getResources(txc) {
  var result = await txc.run(`
        match (r:Resource)
        return collect(r{.*}) as resources
      `);
  return result.records.map((record) => record.get("resources"))[0];
}

async function getAssignments(txc) {
  var result = await txc.run(`
        match (t:Task)-[d:ASSIGNED_TO]->(r:Resource)
        return collect( 
            apoc.map.merge( d{.*}, 
            {event:t.id, resource:r.id})
        ) as assignments
      `);
  return result.records.map((record) => record.get("assignments"))[0];
}

async function getCalendars(txc) {
  var result = await txc.run(`
        match (c:Calendar)
        where not exists { ()-[:HAS_CHILD]->(c)}
        return collect(c{.*, children: [ (c)-[:HAS_CHILD]->(cc:Calendar) | cc{.*, intervals: [ (cc)-[:HAS_INTERVAL]->(ci) | apoc.map.removeKey(properties(ci),'id' ) ]}]}) as calendars 
      `);
  return result.records.map((record) => record.get("calendars"))[0];
}

async function getProject(txc) {
  var result = await txc.run(`
        match (p:Project)
        return p{.*} as project limit 1
      `);
  return result.records.map((record) => record.get("project"))[0];
}

app.post("/sync", async (req, res) => {
  const { requestId, tasks, resources, assignments } = req.body;
  const session = driver.session({ database: DBNAME });
  try {
    const [tasks_res, resources_res, assignments_res] =
      await session.writeTransaction(async (txc) => {
        return await Promise.all([
          syncTasks(txc, tasks),
          syncResources(txc, resources),
          syncAssignments(txc, assignments),
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

async function syncTasks(txc, changes) {
  if (changes) {
    let rows;
    if (changes.added) {
      rows = await createTasks(txc, changes.added);
    }
    if (changes.updated) {
      await updateTasks(txc, changes.updated);
    }
    if (changes.removed) {
      await deleteTasks(txc, changes.removed);
    }
    // if got some new data to update client
    return rows;
  }
}

async function syncResources(txc, changes) {
  if (changes) {
    let rows;
    if (changes.added) {
      rows = await createResources(txc, changes.added);
    }
    if (changes.updated) {
      await updateResources(txc, changes.updated);
    }
    if (changes.removed) {
      await deleteResources(txc, changes.removed);
    }
    // if got some new data to update client
    return rows;
  }
}

async function syncAssignments(txc, changes) {
  if (changes) {
    let rows;
    if (changes.added) {
      rows = await createAssignments(txc, changes.added);
    }
    if (changes.updated) {
      await updateAssignments(txc, changes.updated);
    }
    if (changes.removed) {
      await deleteAssignments(txc, changes.removed);
    }
    // if got some new data to update client
    return rows;
  }
}

async function syncDependencies(txc, changes) {
  if (changes) {
    let rows;
    if (changes.added) {
      rows = await updateDependencies(txc, changes.added);
    }
    if (changes.updated) {
      await updateDependencies(txc, changes.updated);
    }
    if (changes.removed) {
      await deleteDependencies(txc, changes.removed);
    }
    // if got some new data to update client
    return rows;
  }
}

async function createTasks(txc, added) {
  var result = await txc.run(
    `
          unwind $tasks as task
          create (n:Task{id:randomUuid()})
          set n+= apoc.map.removeKeys(task,['baselines','children']) 
          return collect(n{.*}) as tasks
        `,
    { tasks: added }
  );
  return result.records.map((record) => record.get("tasks"))[0];
}

function addChildren(task_id, children, tasks, child_task_rels) {
  if (children) {
    for (task of children) {
      task_copy = Object.assign({}, task);
      delete task_copy["children"];
      tasks.push(task_copy);
      child_task_rels.push({ parent: task_id, child: task_copy.id });
      addChildren(task.id, task.children, tasks, child_task_rels);
    }
  }
}

async function updateTasks(txc, updated) {
  tasks = [];
  child_task_rels = [];
  for (task of updated) {
    const task_copy = structuredClone(task);
    delete task_copy["children"];
    tasks.push(task_copy);
    addChildren(task.id, task.children, tasks, child_task_rels);
  }
  // there is too much going on here
  // split it up so updating tasks, baselines and parent/child tasks
  // are separate concerns?
  var result = await txc.run(
    `
          unwind $tasks as task
          merge (n:Task{id:task.id})
            set n+= apoc.map.removeKeys(task,['baselines'])
          with n, task
          call {
            with n, task
            with n, task
            where task.baselines is not null
            optional match (n)-[r:HAS_BASELINE]->(b)
            delete r, b
            return count(*) as deleted
          }
          foreach ( bl in task.baselines | create (b:Basline) set b+=bl merge (n)-[:HAS_BASELINE]->(b))  
          with collect(task) as tasks
          unwind $rels as rel
          match (p:Task{id:rel.parent}), (c:Task{id:rel.child})
          merge (p)<-[:PARENT_TASK]-(c)
          return tasks
        `,
    { tasks: tasks, rels: child_task_rels }
  );
  return result.records.map((record) => record.get("tasks"))[0];
}

// see comment for update task
async function deleteTasks(txc, removed) {
  var result = await txc.run(
    `
          unwind $tasks as task
          match (n:Task{id:task.id})-[rels:HAS_BASELINE*0..1]->(b)
          foreach (r in rels | delete r)
          detach delete n,b
          return collect(task) as tasks
        `,
    { tasks: removed }
  );
  return result.records.map((record) => record.get("tasks"))[0];
}

async function createResources(txc, added) {
  var result = await txc.run(
    `
          unwind $resources as resource
          create (n:Resource{id:randomUuid()})
          set n+= resource 
          return collect(n{.*}) as resources
        `,
    { resources: added }
  );
  return result.records.map((record) => record.get("resources"))[0];
}

async function updateResources(txc, updated) {
  var result = await txc.run(
    `
          unwind $resources as resource
          merge (n:Resource{id:resource.id})
          set n+= resource 
          return collect(resource) as resources
        `,
    { resources: updated }
  );
  return result.records.map((record) => record.get("resources"))[0];
}

async function deleteResources(txc, removed) {
  var result = await txc.run(
    `
          unwind $resources as resource
          match (n:Resource{id:resource.id})
          detach delete n
          return collect(resource) as resources
        `,
    { resources: removed }
  );
  return result.records.map((record) => record.get("resources"))[0];
}

async function createAssignments(txc, added) {
  var result = await txc.run(
    `
          unwind $assignments as assignment
          with assignment,
          case assignment.id is null
          when true then randomUuid()
          else assignment.id 
          end as assgnId
          merge (t:Task{id: assignment.event}) 
          merge (r:Resource{id: assignment.resource}) 
          merge (t)-[d:ASSIGNED_TO{id:assgnId}]->(r)
          set d.units = assignment.units
          return collect( 
            apoc.map.merge( d{.*}, 
            {event:t.id, resource:r.id})
        ) as assignments
        `,
    { assignments: added }
  );
  return result.records.map((record) => record.get("assignments"))[0];
}

async function updateAssignments(txc, updated) {
  var result = await txc.run(
    `
          unwind $assignments as assignment
          with assignment,
            case assignment.id is null
            when true then randomUuid()
            else assignment.id 
            end as assgnId,
          optional match (t)-[old_assignment:ASSIGNED_TO{id:assignment.id}]->(r)          
          with assignemnt, assignId, old_assignment
          coalesce(assignment.event, t.id) as tid,
          coalesce(assignment.resource, r.id) as rid,
          coalesce(assignment.units, old_assignment.units) as units
          delete old_assignment
          merge (t:Task{id: tid}) 
          merge (r:Resource{id: rid}) 
          merge (t)-[d:ASSIGNED_TO{id:assgnId}]->(r)
          set d.units = assignment.units
          return collect( 
            apoc.map.merge( d{.*}, 
            {event:t.id, resource:r.id})
        ) as assignments 
        `,
    { assignments: updated }
  );
  return result.records.map((record) => record.get("assignments"))[0];
}

async function updateDependencies(txc, updated) {
  var result = await txc.run(
    `
          unwind $dependencies as dependency
          with dependency,
            case dependency.id is null 
              when true then randomUuid() 
              else  dependency.id 
            end as depId,
          coalesce(dependency.fromTask, dependency.from) as fromId,
          coalesce(dependency.toTask, dependency.to) as toId
          merge (t:Task{id: fromId}) 
          merge (r:Task{id:  toId}) 
          merge (t)-[do:DEPENDS_ON{id:depId}]->(r) 
          set do.type=dependency.type, 
              do.lag=dependency.lag, 
              do.lagUnit = dependency.lagUnit
          return collect( 
            apoc.map.merge( do{.*}, 
            {fromTask:t.id, toTask:r.id})
        ) as dependencies
        `,
    { dependencies: updated }
  );
  return result.records.map((record) => record.get("dependencies"))[0];
}

async function deleteDependencies(txc, updated) {
  var result = await txc.run(
    `
          unwind $dependencies as dependency
          match ()-[do:DEPENDS_ON{id:dependency.id}]->()
          delete do
          return collect(dependency) as dependencies
        `,
    { dependencies: updated }
  );
  return result.records.map((record) => record.get("dependencies"))[0];
}

async function updateProject(txc, updated) {
  var result = await txc.run(
    `
          merge (p:Project) //This works as long as there is only one project node
            set p += $project
          return p{.*} as project
        `,
    { project: updated }
  );
  return result.records.map((record) => record.get("project"))[0];
}

async function updateCalendars(txc, updated) {
  var result = await txc.run(
    `
        unwind $calendars as calendar
        with calendar, apoc.map.removeKeys(calendar, ['intervals','children']) as calendarProps
        merge (c:Calendar{id: calendar.id})
        set c += calendarProps
        with c, calendar
        call {
          with c, calendar
          unwind calendar.intervals as interval
          merge (i:Interval{id: calendar.id, recurrentStartDate: interval.recurrentStartDate, recurrentEndDate: interval.recurrentEndDate})
            set i += interval
          merge (c)-[:HAS_INTERVAL]->(i)
          return count(*) as temp
        }
        with c, calendar
        unwind calendar.children as child
        with c,calendar, child, apoc.map.removeKeys(child, ['intervals','children']) as childProps
        merge (cc:Calendar{id: child.id})
        set cc += childProps
        with c, cc, child
        merge (c)-[:HAS_CHILD]->(cc)
        with c,cc,child
        foreach (cinterval in child.intervals | 
          merge (ci:Interval{id: child.id, recurrentStartDate: cinterval.recurrentStartDate, recurrentEndDate: cinterval.recurrentEndDate})
            set ci += cinterval
          merge (cc)-[:HAS_INTERVAL]->(ci)
        )
        with count(1) as dummy
        return $calendars as calendars   
        `,
    { calendars: updated }
  );
  return result.records.map((record) => record.get("calendars"))[0];
}

async function deleteAssignments(txc, removed) {
  var result = await txc.run(
    `
          unwind $assignments as assignment
          match ()-[old_assignment:ASSIGNED_TO{id:assignment.id}]->()
          delete old_assignment
          return collect(assignment) as assignments
        `,
    { assignments: removed }
  );
  return result.records.map((record) => record.get("assignments"))[0];
}

module.exports = {
  updateProject,
  updateCalendars,
  updateTasks,
  updateResources,
  updateDependencies,
  createAssignments,
};
