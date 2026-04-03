import { useMemo } from "react";
import Prism from "prismjs";
import "prismjs/components/prism-json";
import "prismjs/components/prism-bash";
import "prismjs/components/prism-python";
import "prismjs/components/prism-javascript";
import "prismjs/components/prism-typescript";
import "prismjs/components/prism-rust";
import { CopyButton } from "../copy-button/copy-button.js";

export interface CodeBlockProps {
  code: string;
  language?: string;
  showCopy?: boolean;
  className?: string;
}

export function CodeBlock({ code, language = "bash", showCopy = true, className = "" }: CodeBlockProps) {
  const highlightedHtml = useMemo(() => {
    if (code.length >= 50_000 || !Prism.languages[language]) return null;
    return Prism.highlight(code, Prism.languages[language], language);
  }, [code, language]);

  return (
    <div className={`group relative bg-code-bg ${className}`}>
      {showCopy && (
        <div className="absolute right-2 top-2 opacity-0 transition-opacity group-hover:opacity-100">
          <CopyButton text={code} />
        </div>
      )}
      <pre className={`language-${language} !bg-code-bg !m-0`}>
        {highlightedHtml != null ? (
          <code className={`language-${language}`} dangerouslySetInnerHTML={{ __html: highlightedHtml }} />
        ) : (
          <code className={`language-${language}`}>{code}</code>
        )}
      </pre>
    </div>
  );
}
