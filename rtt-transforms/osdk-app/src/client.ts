/** OSDK client + OAuth (used in production / Developer-Console-hosted mode).
 *  For REST/dev mode the app uses a bearer token from .env instead (see osdk.ts). */
import { createClient } from '@osdk/client';
import { createPublicOauthClient } from '@osdk/oauth';

const url = import.meta.env.VITE_FOUNDRY_API_URL as string;
const clientId = import.meta.env.VITE_FOUNDRY_CLIENT_ID as string;
const redirectUrl = import.meta.env.VITE_FOUNDRY_REDIRECT_URL as string;
const ontologyRid = import.meta.env.VITE_FOUNDRY_ONTOLOGY_RID as string;

export const auth = createPublicOauthClient(clientId, url, redirectUrl);

/** Typed OSDK client. Pass to <OsdkProvider client={client}> or use directly. */
const client = createClient(url, ontologyRid, auth);
export default client;
