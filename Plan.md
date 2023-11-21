Plan to go away from the prefect dbt core blocks & connection blocks

### Motivation

- The prefect blocks dont actually add any value and have just increased the complexity through out the app. Lot convoluted relationships need to be formed/used to work with blocks. The only use of the block is to run dbt cli operations and airbyte syncs.

- Becomes easier/flexible for us to parameterize dbt cli commands.

- Also, it setups us up for to scale for multiple warehouses and multiple dbt targets. People will be able to create a development pipeline and a prod pipeline.

### Idea

- Idea is that we will create block objects in memory, run them and clear them. We wont save them

- Only `dbt cli profile` and `airbyte server` blocks will be created to store warehouse information and airbyte server host details.

- We will also create `secret` block(s) in prefect to store github token if they want to connect to a private repo.

- We can go away with `airbyte server` block & take the creds from env. Since all orgs have the same airbyte server. Or we can create a single server block for the entire app.

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

### Block lock logic replacement

- Basically we will replace blocks by commands that we run. We will now have a command lock logic.

- Each deployment will be running some command which can be locked or not.

### Structural & schema changes

- `ddpui_orgprefectblock` will have very minimal use in the entire. No block lock logic here. We will store airbyte server, dbt cli profile and secret blocks here.

- We will have to store the name of the dbt cli profile block name. Add a col in table `ddpui_orgdbt` to store this. Whenever a block runs, we pass directory information & the cli profile block name to run with.

- Either we hardcode a list of commands or create a master table `ddpui_tasks` to store the commands/tasks available in our app. It will have master list of airbyte syncs, git pull & dbt core commands. The table will have the following columns

  - id
  - slug
  - command

- Add a jsonb col `params` in `ddpui_dataflowblock` to store deployment parameters of each deployment.

- `ddpui_dataflowblock` will go away and we create a new table instead `ddpui_dataflowtask`. Each deployment will now be mapped to a task from our master table.

- `ddpui_blocklock` becomes `ddpui_locktask`. This will be the table that stores the locks when tasks are running or triggered. Prefect webhook will now update this.

### Deployment plan

- The idea is to make sure things currently running dont break. Since we wont be doing pushing UI changes but only backend in the release.

- Both the old block and new tasks logic should work simultaenously. Until we release the UI changes.

- We won't touch the old apis. We will version & create new ones. Same goes for the flows in prefect. Everything will be deprecated once we move away with the blocks completely.

- Final step is to migrate the current setup. For this we will write django commands(scripts) that help us go away with the blocks.

### Tasks/steps

#### Setup the api to save dbt cli profile

- New api in prefect proxy to create dbt cli profile block `/proxy/blocks/dbtcli/profile/`

- Api in Django backend `/blocks/dbtcli/profile/`. This api will be hit instead of `/blocks/dbt/`
