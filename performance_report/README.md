# elock performance report

The report reads completed Common Test points from `_build/test/logs`.

```bash
cd performance_report
npm install
npm run dev
```

Development mode serves the application at `http://localhost:5173`. It starts
the data backend on port 3000 and refreshes the page data every five seconds.

For a production-style local build:

```bash
npm run build
npm start
```

The built application is served at `http://localhost:3000`. From the project
root, `make performance_report` installs, builds, starts it and opens the page.
