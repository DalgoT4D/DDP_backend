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

### Change log

#### <u>User org client related</u>

| Before                                                                     | After                                                                                     |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Django: create airbyte workspace `POST /api/airbyte/workspace/`            | Django : new api to create airbyte workspace `POST /api/airbyte/v1/workspace/`            |
| Django: create organization `POST /api/organizations/`                     | Django : new api to create org `POST /api/v1/organizations/`                              |
| Proxy: create organization warehouse `POST /api/organizations/warehouses/` | Proxy : new api to create organization warehouse `POST /api/v1/organizations/warehouses/` |

#### <u>Airbyte connections</u>

| Before                                                                                                  | After                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Django: create airbyte conncetion `POST /api/airbyte/connections/`                                      | Django : new api to create airbyte connection `POST /api/airbyte/v1/connections/`                                                             |
| Django: to sync a connection via the button `POST /api/airbyte/connections/{connection_block_id}/sync/` | Django : reusing api to sync connection via deployment (already created in prefect_api) `POST /api/prefect/v1/flows/{deployment_id}/flow_run` |
| Get all connections `GET /api/airbyte/connections`                                                      | Get all connections `GET /api/airbyte/v1/connections`                                                                                         |
| Get a connection `POST /api/airbyte/connections/:connection_block_id`                                   | Get a connection `POST /api/airbyte/v1/connections/:connection_id`                                                                            |
| Reset a connection `POST /api/airbyte/connections/:connection_block_id/reset`                           | Reset a connection `POST /api/airbyte/v1/connections/:connection_id/reset`                                                                    |
| Update a connection `PUT /api/airbyte/connections/:connection_block_id/update`                          | Update a connection `PUT /api/airbyte/v1/connections/:connection_id/update`                                                                   |
| Delete a connection `DELETE /api/airbyte/connections/:connection_block_id`                              | Delete a connection `DELETE /api/airbyte/v1/connections/:connection_id`                                                                       |

#### <u>Blocks on transform page</u>

| Before                                                        | After                                                                                      |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Django: create dbt blocks api `POST /api/prefect/blocks/dbt/` | Django : api to create org tasks & cli profile blocks `POST /api/prefect/tasks/transform/` |
| Django: get dbt blocks api `GET /api/prefect/blocks/dbt/`     | Django : api to get org tasks `GET /api/prefect/tasks/transform/`                          |
| Proxy: create dbt core block api `/proxy/blocks/dbtcore/`     | Proxy : new api in proxy to create dbt cli profile block `/proxy/blocks/dbtcli/profile/`   |

#### <u>Running blocks on transform page</u>

| Before                                                                                                                                            | After                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Django: apis to run dbt core operations & git pull operations on transform can be deprecated `/api/prefect/flows/dbt_run/` & `/api/dbt/git_pull/` | Django : api to run tasks (dbt + shell operation(git pull)) `/api/prefect/tasks/{orgtask_id}/run/` <br /> Proxy : new api to run tasks (dbt tasks) via updated flows `/proxy/v1/flows/dbtcore/run/` <br /> Proxy : new api to run shell operations (a general shell op; will be used for git pull for now) via updated flows `/proxy/flows/shell/run/` |
| Django: api to run long tasks via deployment `/api/prefect/flows/{deployment_id}/flow_run`                                                        | Django: new api to run long running tasks via deployment `/api/prefect/v1/flows/{deployment_id}/flow_run`                                                                                                                                                                                                                                              |
