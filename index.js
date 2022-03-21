/**
 * This exercise has you implement a synchronize() method that will
 * copy all records from the sourceDb into the targetDb() then start
 * polling for changes. Places where you need to add code have been
 * mostly marked, and the goal is to get the runTest() to complete
 * successfully.
 *
 *
 * Requirements:
 *
 * Try to solve the following pieces for a production system. Note,
 * doing one well is better than doing all poorly.
 *
 * 1. syncAllNoLimit(): Make sure what is in the source database is
 *    in our target database for all data in one sync. Think of a naive
 *    solution.
 * 2. syncAllSafely(): Make sure what is in the source database is in
 *    our target database for all data in batches during multiple
 *    syncs. Think of a pagination solution.
 * 3. syncNewChanges(): Make sure updates in the source database is in
 *    our target database without syncing all data for all time. Think
 *    of a delta changes solution.
 * 4. synchronize(): Create polling for the sync. A cadence where we
 *    check the source database for updates to sync.
 *
 * Feel free to use any libraries that you are comfortable with and
 * change the parameters and returns to solve for the requirements.
 *
 *
 * You will need to reference the following API documentation:
 *
 * Required: https://www.npmjs.com/package/nedb
 * Required: https://github.com/bajankristof/nedb-promises
 * Recommended: https://lodash.com/docs/4.17.15
 * Recommended: https://www.npmjs.com/package/await-the#api-reference
 */
const Datastore = require("nedb-promises");
const _ = require("lodash");
const the = require("await-the");
const companies = require("./mock/companies");
const schedule = require("node-schedule");
const { performance } = require("perf_hooks");

// The source database to sync updates from.
const sourceDb = new Datastore({
  inMemoryOnly: true,
  timestampData: true,
});

// The target database that sendEvents() will write too.
const targetDb = new Datastore({
  inMemoryOnly: true,
  timestampData: true,
});

let TOTAL_RECORDS;
let EVENTS_SENT = 0;

const load = async () => {
  let startTime = performance.now();

  // Add some documents to the collection.
  // TODO: Maybe dynamically do this? `faker` might be a good library here.
  _.forEach(companies, async function (company) {
    await sourceDb.insert(company);
    await the.wait(300);
  });

  let endTime = performance.now();

  TOTAL_RECORDS = companies.length;

  console.log(
    `------------------------------------------------------------------`
  );
  console.log(
    `Call to load initial data took ${endTime - startTime} milliseconds`
  );
  console.log(
    `It has imported a total of ${TOTAL_RECORDS} records to SourceDB`
  );
  console.log(
    `------------------------------------------------------------------`
  );
};

/**
 * API to send each document to in order to sync.
 */
const sendEvent = async (data) => {
  EVENTS_SENT += 1;

  console.log("event being sent...");

  // TODO: Write data to targetDb
  return await targetDb.insert(data);
};

// Find and update an existing document.
const touch = async (name) => {
  await sourceDb.update({ name }, { $set: { owner: "test4" } });
};

/**
 * Utility to log one record to the console for debugging.
 */
const read = async (name) => {
  const data = await sourceDb.findOne({ name });
  return data;
};

/**
 * Get all records out of the database and send them using
 * 'sendEvent()'.
 */
const syncAllNoLimit = async () => {
  //Loading initial data
  await load();

  // Check what the saved data looks like.
  // await read("GE");
  EVENTS_SENT = 0;

  let dataFromSourceDb = await sourceDb.find({});

  sendEvent(dataFromSourceDb)
    .then(() => {
      console.log(`[TargetDB] is syncing with SourceDB...`);
    })
    .catch((error) => {
      console.log(error);
    });

  console.log("-----------------------------------------------");
  console.log("--- [TargetDB] has been successfully synced with SourceDB ---");
  console.log(`---- TOTAL RECORDS SYNCED: ${TOTAL_RECORDS} records ----`);
  console.log("-----------------------------------------------");

  synchronize(); //keep up running the synchronization process

  return true;
};

/**
 * Sync up to the provided limit of records. Data returned from
 * this function will be provided on the next call as the data
 * argument.
 */
const syncWithLimit = async (limit, next, data) => {
  next = next + limit * data.lastResultSize;

  await sourceDb
    .find({})
    .limit(limit)
    .skip(next)
    .then((data) => {
      // Inserting each pagination
      sendEvent(data)
        .then(() => {
          console.log(`[TargetDB] is syncing with SourceDB...`);
        })
        .catch((error) => {
          console.log(error);
        });
    })
    .catch((err) => {
      console.log(err);
    });

  data.lastResultSize += 1;

  return data;
};

/**
 * Synchronize in batches.
 */
const syncAllSafely = async (batchSize, countFromTargetDB, data) => {
  EVENTS_SENT = 0;
  if (_.isNil(data)) {
    data = {};
  }
  // While loop Break Statement
  data.lastResultSize = 0;

  let totalPages = Math.round(countFromTargetDB / batchSize).toFixed();

  let next = 0;

  await the.while(
    () => data.lastResultSize < totalPages,
    async () => await syncWithLimit(batchSize, next, data)
  );

  console.log("-----------------------------------------------");
  console.log("--- [TargetDB] has been successfully synced with SourceDB ---");
  console.log(`---- TOTAL RECORDS SYNCED: ${TOTAL_RECORDS} records ----`);
  console.log("-----------------------------------------------");

  return data;
};

/**
 * Sync changes since the last time the function was called with
 * with the passed in data.
 */
const syncNewChanges = async (data) => {
  console.log("Syncing databases with NEW changes, please wait...");

  //look for record with changes on sourceDB and update changes on TargetDB
  _.forEach(data, async function (companyName) {
    let dataFromSourceDb = await sourceDb.find({ name: companyName });
    await targetDb
      .update({ name: companyName }, { $set: dataFromSourceDb[0] }, {})
      .then(() => {
        console.log(`Sychronize: [TargetDB] is up to date!`);
      });
  });

  return true;
};

/**
 * Implement function to fully sync of the database and then
 * keep polling for changes.
 */
const synchronize = async () => {
  let company = await read(companies[0].name);
  let companyName = company.name;
  let pendingChanges = 0;

  //waiting 10 seconds before updating a record on sourceDB
  setTimeout(async () => {
    await sourceDb.update(
      { name: companyName },
      { $set: { owner: "Juan" } },
      {}
    );
  }, 10000);

  const job = schedule.scheduleJob("*/5 * * * * *", async function () {
    //this run every 5 seconds
    let dataFromSourceDB = await sourceDb.find();
    let dataFromTargetDB = await targetDb.find();
    let dataFromSource = await sourceDb.find({ name: companyName });
    let dataFromTarget = await targetDb.find({ name: companyName });
    let idsToUpdate = [];

    for (let i = 0; i < dataFromTargetDB.length; i++) {
      if (dataFromSourceDB[i].updatedAt > dataFromTargetDB[i].updatedAt) {
        console.log("----- Record with changes on SourceDB -------");
        console.log(dataFromSource);

        console.log("----- Current record on TargetDB -------");
        console.log(dataFromTarget);

        pendingChanges = 1;
        idsToUpdate.push(dataFromTargetDB[i].name);
      }
    }

    pendingChanges === 0
      ? console.log("Synchronize: Everything is up to date.")
      : console.log("Synchronize: [TargetDB] has some PENDING changes.");

    if (pendingChanges === 1) {
      job.cancel();
      pendingChanges = 0;
      await syncNewChanges(idsToUpdate).then(() => {
        synchronize(); //calling the method again
      });
    }
  });
};

/**
 * Simple test construct to use while building up the functions
 * that will be needed for synchronize().
 */
const runTest = async () => {
  await load();

  // TODO: Maybe use something other than logs to validate use cases?
  // Something like `assert` or `chai` might be good libraries here.
  if (EVENTS_SENT === TOTAL_RECORDS) {
    console.log("1. synchronized correct number of events");
  }

  EVENTS_SENT = 0;

  let dataFromSource = await sourceDb.find({});
  let countFromSourceDB = dataFromSource.length;

  await syncAllSafely(5, countFromSourceDB);

  if (EVENTS_SENT === TOTAL_RECORDS) {
    console.log("2. synchronized correct number of events");
  }

  // Make some updates and then sync just the changed files.
  EVENTS_SENT = 0;
  // await the.wait(300);
  // await touch(companies[0].name);
  // await syncNewChanges(1, data);

  if (EVENTS_SENT === 1) {
    console.log("3. synchronized correct number of events");
  }

  //Call synchronize()
  synchronize();
};

//runTest() will run syncAllSafely() and synchronize() to see them working
runTest();

//NOTE: Call syncAllNoLimit() to test it out, get rid of the comment below
//syncAllNoLimit();
