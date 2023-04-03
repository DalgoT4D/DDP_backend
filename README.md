## DDP_backend

Django application for the DDP platform's management backend. Exposes API endpoints for the management frontend to communicate with, for the purposes of

-   Onboarding an NGO client
-   Adding users from the client-organization
-   Creating a client's workspace in our Airbyte installation
-   Configuring that workspace i.e. setting up sources, destinations and connections
-   Configuring data ingest jobs in our Prefect setup
-   Connecting to the client's dbt GitHub repository
-   Configuring dbt run jobs in our Prefect setup

## Development conventions

### Api end points naming

-   REST conventions are being followed.
-   CRUD end points for a User resource would look like:
    -   GET <mark>/api/users/</mark>
    -   GET <mark>/api/users/user_id</mark>
    -   POST <mark>/api/users/</mark>
    -   PUT <mark>/api/users/:user_id</mark>
    -   DELETE <mark>/api/users/:user_id</mark>

### Api handlers / controller functions naming

-   Handlers would be named in <mark>lowerCamelCase</mark> fashion where the lower case word would represent the api end point followed by the resource/feature/functionality that the api is implementing. Plural name should be used depending on the api.
    -   Eg handler for an api to create User would look like
        <mark>postUser</mark>
    -   Eg handler for an api to create OrganizationClient would look like
        <mark>postOrganizationClient</mark>
    -   Eg handler for an api to fetch all Users would look like
        <mark>getUsers</mark>
    -   Eg handler for a login api would look like <mark>postLogin</mark>

## Setup instructions

-   `pip install -r requirements.txt`

-   create .env from .env.template
