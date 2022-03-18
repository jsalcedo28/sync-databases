const { faker } = require("@faker-js/faker");

//initializing companies array
const companies = [];

//set the size of data you would like to load
const length = 100;

for (let i = 0; i < length; i++) {
  companies.push({
    name: faker.company.companyName(),
    owner: faker.name.firstName(),
    amount: faker.datatype.number(),
  });
}

module.exports = companies;
