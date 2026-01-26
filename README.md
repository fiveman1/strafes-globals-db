# strafes-globals-db

Globals/world record tracking for StrafesNET games. Designed to be run hourly. I recommend running this at the 5th minute of every hour to make sure the API is finished updating (which runs hourly at the top of every hour).

This tool will store all globals from the StrafesNET API into a local MySQL database.

### Setup

`npm install`

Make a `.env` in the root directory (this folder) with the following:

```sh
STRAFES_KEY=<your key>
DB_USER=<your MySQL user>
DB_PASSWORD=<your MySQL password>
```

Install MySQL and create a database named `strafes_globals`.

### Run dev

`npm run dev`

To seed (download all WRs at once):`npm run dev-seed`

### Run prod

`npm run build`

`npm run start`

To seed (download all WRs at once): `npm run seed`
