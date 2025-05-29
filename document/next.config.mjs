import {createMDX} from 'fumadocs-mdx/next';

const withMDX = createMDX();
const repoName = "Signal";


/** @type {import('next').NextConfig} */
const config = {
    reactStrictMode: true,
    output: 'export',
    basePath: `/${repoName}`,
    assetPrefix: `/${repoName}`,
};

export default withMDX(config);
