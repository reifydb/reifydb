declare module '*.module.css' {
  const classes: { readonly [key: string]: string };
  export default classes;
}

declare module '@theme/Layout' {
  import { ReactNode } from 'react';
  export interface LayoutProps {
    title?: string;
    description?: string;
    children: ReactNode;
  }
  export default function Layout(props: LayoutProps): JSX.Element;
}

declare module '@theme/Heading' {
  import { ReactNode } from 'react';
  export interface HeadingProps {
    as: 'h1' | 'h2' | 'h3' | 'h4' | 'h5' | 'h6';
    children: ReactNode;
    className?: string;
  }
  export default function Heading(props: HeadingProps): JSX.Element;
}

declare module '@docusaurus/Link' {
  import { ReactNode } from 'react';
  export interface LinkProps {
    to: string;
    className?: string;
    children: ReactNode;
  }
  export default function Link(props: LinkProps): JSX.Element;
}

declare module '@docusaurus/useDocusaurusContext' {
  export interface DocusaurusContext {
    siteConfig: {
      title: string;
      tagline: string;
      url: string;
      baseUrl: string;
      organizationName: string;
      projectName: string;
    };
  }
  export default function useDocusaurusContext(): DocusaurusContext;
}

declare module '@docusaurus/BrowserOnly' {
  import { ReactNode } from 'react';
  export interface BrowserOnlyProps {
    children: () => ReactNode;
    fallback?: ReactNode;
  }
  export default function BrowserOnly(props: BrowserOnlyProps): JSX.Element;
}
