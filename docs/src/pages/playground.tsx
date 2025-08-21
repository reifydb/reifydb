import React from 'react';
import Layout from '@theme/Layout';
import BrowserOnly from '@docusaurus/BrowserOnly';

function PlaygroundContent() {
  return (
    <BrowserOnly fallback={<div>Loading...</div>}>
      {() => {
        const Playground = require('../playground/Playground').default;
        return <Playground />;
      }}
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