import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './styles.css';
// When using @osdk/react hooks, wrap App:
//   import { OsdkProvider } from '@osdk/react';
//   import client from './client';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    {/* <OsdkProvider client={client}> */}
    <App />
    {/* </OsdkProvider> */}
  </React.StrictMode>,
);
