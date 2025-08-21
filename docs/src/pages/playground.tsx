import React, { lazy, Suspense } from 'react';
import Layout from '@theme/Layout';
import BrowserOnly from '@docusaurus/BrowserOnly';

const Playground = lazy(() => import('../playground/Playground'));

function PlaygroundContent() {
  return (
    <BrowserOnly fallback={<div>Loading...</div>}>
      {() => (
        <Suspense fallback={<div>Loading playground...</div>}>
          <Playground />
        </Suspense>
      )}
    </BrowserOnly>
  );
}

export default function PlaygroundPage(): JSX.Element {
  return (
    <Layout
      title="Playground"
      description="Interactive ReifyDB Playground - Try SQL queries in your browser"
    >
      <PlaygroundContent />
    </Layout>
  );
}
