Plan to go away from the prefect dbt core blocks & connection blocks

### Motivation

The prefect blocks dont actually add any value and have just increased the complexity through out the app. Lot convoluted relationships need to be formed/used to work with blocks. The only use of the block is to run dbt cli operations and airbyte syncs.

### Idea

- Idea is that we will create block objects in memory, run them and clear them. We wont save them

- Only `dbt cli profile` and `airbyte server` blocks will be created to store warehouse information and airbyte server host details.

- Run git pull command through `ShellOperation` class. `shell_op.run()`. To run a shell operation we need

  - commands
  - env
  - work_dir

- Run dbt command through `DbtCoreOperation` class. `dbt_op.run()`. To run a core operation we need

  - commands
  - cli profile block name
  - project_dir
  - work_dir
  - profiles_dir
  - env if any

- Run airbyte sync by creating an object of `AirbyteConnection` class and then using run sync function `run_connection_sync(airbyte_connection)`. To run an airbyte sync we need

  - airbyte server block name
  - connection_id
  - timeout

### Structural & schema changes

- `ddpui_orgprefectblock` will go away.

- We will have to store the name of the dbt cli profile block name. Add a col in table `ddpui_orgdbt` to store this. Whenever a block runs, we pass directory information & the cli profile block name to run with.

- Either we hardcode a list of commands or create a master table `ddpui_dbtcmds` to store the commands available in our app.

-

### Tasks/steps

- `/`
