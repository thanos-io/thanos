import { Dispatch, SetStateAction, useEffect, useState } from 'react';

// split out so class components (which can't use hooks) can still go through
// a shared accessor instead of touching localStorage directly
export function getStorageItem<S>(localStorageKey: string, initialState: S): S {
  try {
    return JSON.parse(localStorage.getItem(localStorageKey) || JSON.stringify(initialState));
  } catch {
    // a foreign or corrupted value under this key shouldn't crash the caller
    return initialState;
  }
}

export function setStorageItem<S>(localStorageKey: string, value: S): void {
  localStorage.setItem(localStorageKey, JSON.stringify(value));
}

export function hasStorageItem(localStorageKey: string): boolean {
  return localStorage.getItem(localStorageKey) !== null;
}

export function useLocalStorage<S>(localStorageKey: string, initialState: S): [S, Dispatch<SetStateAction<S>>] {
  const [value, setValue] = useState(getStorageItem(localStorageKey, initialState));

  useEffect(() => {
    setStorageItem(localStorageKey, value);
  }, [localStorageKey, value]);

  return [value, setValue];
}
