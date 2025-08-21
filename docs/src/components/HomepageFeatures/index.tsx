import type { ReactNode } from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  Icon: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Easy to Use',
    Icon: '🚀',
    description: (
      <>
        ReifyDB was designed from the ground up to be easily installed and used to get your database
        up and running quickly.
      </>
    ),
  },
  {
    title: 'Focus on What Matters',
    Icon: '🎯',
    description: (
      <>
        ReifyDB lets you focus on your data, and we&apos;ll handle the complexity. Enjoy real-time
        queries and automatic optimizations.
      </>
    ),
  },
  {
    title: 'Modern Architecture',
    Icon: '⚡',
    description: (
      <>
        Built with performance in mind. ReifyDB leverages modern techniques for blazing fast queries
        and real-time data synchronization.
      </>
    ),
  },
];

function Feature({ title, Icon, description }: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <div className={styles.featureIcon}>{Icon}</div>
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
