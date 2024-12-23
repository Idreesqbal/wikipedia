
# dbt Project Repository

This repository contains a **dbt** (Data Build Tool) project for transforming and analyzing data using a modern data stack. It includes setup files, dependencies, and configurations for running dbt in your environment.

---

## 📂 Project Structure

```
wikipedia/
├── dbt_project.yml       # Main dbt project configuration file
├── profiles.yml          # Profiles for database connections
├── packages.yml          # dbt dependencies configuration
├── .gitignore            # Git ignore file
├── .env                  # Environment variables for sensitive data (excluded from Git)
├── README.md             # Project documentation
├── models/               # dbt models
│   ├── int/              # Intermediate models
│   ├── pub/              # Published models
│   └── stg/              # Staging models
├── macros/               # Reusable macros for dbt
├── tests/                # Custom tests for dbt models
├── snapshots/            # Snapshots for slowly changing dimensions
├── seeds/                # Seed files for static datasets
├── logs/                 # Log files generated by dbt
├── target/               # Compiled and runtime files (excluded from Git)
└── dbt_packages/         # Installed dbt packages (auto-generated, excluded from Git)
el/                   # Folder for Extract and Load scripts
│   ├── medialink_api.ipynb # Extraction and loading script from MediaWIKI API to PostgreSQL
│   ├── .env # Environment variables for sensitive data (excluded from Git)
```

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed on your system:
- Python 3.8+
- dbt (installed via pip or Homebrew)
- A supported dbt database (e.g., BigQuery, Snowflake, Redshift, or **Postgres** ) 
- PostgreSQL installed and configured locally or on a server
- Git (for version control)

### Clone the Repository

```bash
git clone https://github.com/Idreesqbal/wikipedia
cd your-repo-name
```

### Install Dependencies

1. Install Python dependencies:
   ```bash
   pip install dbt-postgres  # Or your respective dbt adapter (e.g., dbt-snowflake, dbt-bigquery)
   ```

2. Install dbt packages:
   ```bash
   dbt deps
   ```

---

## ⚙️ Configuration

### `dbt_project.yml`
This file contains the core configuration for your dbt project, such as the project name, model paths, and database configurations. Modify it as needed.

### `profiles.yml`
Update the `profiles.yml` file to include your database credentials. Example for postgreSQL:
```yaml
default:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost        # Replace with your PostgreSQL host (e.g., localhost or server IP)
      user: your_user        # Replace with your PostgreSQL username
      password: your_password  # Replace with your PostgreSQL password
      port: 5432             # Default PostgreSQL port (replace if using a custom port)
      dbname: your_database  # Replace with your database name
      schema: public         # Schema where dbt will write tables and views

```

### `packages.yml`
Add dependencies to extend dbt functionality, like dbt-utils or third-party packages. Run `dbt deps` after making changes.

---

## 🛠️ Usage

### Run dbt Models

1. Compile models:
   ```bash
   dbt compile
   ```

2. Run transformations:
   ```bash
   dbt run
   ```

3. Test the models:
   ```bash
   dbt test
   ```

### Generate Documentation

Generate and serve dbt documentation:
```bash
dbt docs generate
dbt docs serve
```

---

## 🧩 Dependencies

This project uses the following dbt packages (defined in `packages.yml`):
- **dbt-utils**: Utility macros for common transformations.
- **codegen**: needed to automatically generate yaml

---

## 🤝 Contributing

Contributions are welcome! To contribute:
1. Fork the repository.
2. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add your message"
   ```
4. Push and create a pull request.

---

## 📝 License

This project is public, no license required.

---

## 📧 Contact

For questions or support, reach out to idreesqbal@gmail.com
