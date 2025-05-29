import defaultMdxComponents from 'fumadocs-ui/mdx';
import type {MDXComponents} from 'mdx/types';
import {Mermaid} from '@/components/mdx/mermaid';
import * as FilesComponents from 'fumadocs-ui/components/files';
import * as TabsComponents from 'fumadocs-ui/components/tabs';
import {Accordion, Accordions} from 'fumadocs-ui/components/accordion';
import * as icons from 'lucide-react';
import {CodeBlock, Pre} from 'fumadocs-ui/components/codeblock';
import {Callout} from 'fumadocs-ui/components/callout';
import {TypeTable} from 'fumadocs-ui/components/type-table';
import {GithubInfo} from 'fumadocs-ui/components/github-info';

// use this function to get MDX components, you will need it for rendering MDX
export function getMDXComponents(components?: MDXComponents): MDXComponents {
    return {
        ...(icons as unknown as MDXComponents),
        ...defaultMdxComponents,
        // HTML `ref` attribute conflicts with `forwardRef`
        pre: ({ref: _ref, ...props}) => (
            <CodeBlock {...props}>
                <Pre>{props.children}</Pre>
            </CodeBlock>
        ),
        TypeTable,
        Mermaid,
        ...TabsComponents,
        ...FilesComponents,
        Accordion,
        Accordions,
        Callout,
        ...components,
        GithubInfo
    };
}
