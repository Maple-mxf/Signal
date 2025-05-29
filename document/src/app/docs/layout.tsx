import {DocsLayout, type DocsLayoutProps} from 'fumadocs-ui/layouts/docs';
import type {ReactNode} from 'react';
import {baseOptions} from '@/app/layout.config';
import {source} from '@/lib/source';


// const docsOptions: DocsLayoutProps = {
//     ...baseOptions,
//     links: [
//         {
//             type: 'custom',
//             children: (
//                 <GithubInfo owner="Maple-mxf" repo="Signal" className="lg:-mx-2"/>
//             ),
//         },
//     ],
// };

export default function Layout({children}: { children: ReactNode }) {
    return (
        <DocsLayout tree={source.pageTree}   >
                    {children}
        </DocsLayout>
    );
}
