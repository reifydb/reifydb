// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './app.tsx'
import '@fontsource/archivo-black/index.css'
import '@fontsource-variable/ibm-plex-sans/index.css'
import '@fontsource-variable/jetbrains-mono/index.css'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
        <App/>
    </React.StrictMode>,
)
