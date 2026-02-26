import { supabase } from './supabase';

export async function handleAuthError(error: unknown): Promise<void> {
  if (!error) return;

  const errorMessage = error instanceof Error ? error.message : String(error);

  // Only treat as auth error if it's specifically about JWT/token expiration
  // NOT for general 401 errors which could come from Edge Functions
  const isAuthError =
    (errorMessage.includes('JWT') && errorMessage.includes('expired')) ||
    errorMessage.includes('refresh_token_not_found') ||
    errorMessage.includes('invalid_grant') ||
    (errorMessage.includes('JWT') && errorMessage.includes('invalid'));

  if (isAuthError) {
    console.warn('Auth error detected, signing out:', errorMessage);
    try {
      await supabase.auth.signOut();
    } catch (signOutError) {
      console.error('Error during forced sign out:', signOutError);
    }

    if (window.location.pathname !== '/login' && window.location.pathname !== '/') {
      window.location.href = '/login';
    }
  }
}
