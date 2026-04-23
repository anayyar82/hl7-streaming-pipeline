import { createBrowserRouter, RouterProvider, NavLink, Outlet } from 'react-router';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@databricks/appkit-ui/react';

const navLinkClass = ({ isActive }: { isActive: boolean }) =>
  `px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
    isActive
      ? 'bg-primary text-primary-foreground'
      : 'text-muted-foreground hover:bg-muted hover:text-foreground'
  }`;

function Layout() {
  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="border-b px-6 py-3 flex items-center justify-between gap-4">
        <div className="flex items-center gap-4">
          <h1 className="text-lg font-semibold text-foreground">HL7App</h1>
          <span className="text-xs text-muted-foreground font-medium uppercase tracking-wide">
            AppKit
          </span>
        </div>
        <nav className="flex gap-1">
          <NavLink to="/" end className={navLinkClass}>
            Home
          </NavLink>
        </nav>
      </header>

      <main className="flex-1 p-6">
        <Outlet />
      </main>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [{ path: '/', element: <HomePage /> }],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}

function HomePage() {
  return (
    <div className="max-w-3xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-foreground tracking-tight">
          ED &amp; ICU operations
        </h2>
        <p className="text-muted-foreground mt-1">
          TypeScript · React 19 · Vite — Databricks AppKit. Lakebase, Genie, and job IDs are wired
          via <code className="text-sm bg-muted px-1 rounded">app.yaml</code> (same values as the
          Streamlit app). Port dashboards and APIs here incrementally.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Reference path</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm text-muted-foreground">
          <p>
            <strong className="text-foreground">Landing</strong> → DLT (bronze/silver/gold) → ML
            features &amp; predictions → <strong className="text-foreground">Lakebase</strong> →
            this app (and Genie).
          </p>
          <p>
            The classic <strong className="text-foreground">Streamlit</strong> UI remains in{' '}
            <code className="text-xs bg-muted px-1 rounded">hl7-forecasting-app/</code> until
            feature parity; both can run as separate Databricks Apps.
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Next steps (developers)</CardTitle>
        </CardHeader>
        <CardContent>
          <ul className="list-disc pl-5 space-y-1 text-sm text-muted-foreground">
            <li>
              Add AppKit plugins in <code className="text-xs bg-muted px-1 rounded">server/server.ts</code>{' '}
              (e.g. <code className="text-xs">lakebase()</code>, <code className="text-xs">genie()</code>){' '}
              per{' '}
              <a
                className="text-primary underline"
                href="https://databricks.github.io/appkit/docs/"
                target="_blank"
                rel="noreferrer"
              >
                AppKit docs
              </a>
              .
            </li>
            <li>Recreate each Streamlit page as a React route under <code className="text-xs bg-muted px-1 rounded">client/src/</code>.</li>
            <li>Run <code className="text-xs bg-muted px-1 rounded">npm run build</code> before deploy.</li>
          </ul>
        </CardContent>
      </Card>
    </div>
  );
}
