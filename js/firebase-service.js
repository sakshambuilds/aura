/**
 * Aura - Firebase & Global Realtime Synchronization Service
 * Provides Authentication, Cloud Persistence, Cross-Device Sync & Resilient Multi-User Storage.
 * Connects all devices and accounts to a shared real-time database so reels, posts, stories,
 * profiles, comments and likes uploaded from ANY device/account are immediately visible to EVERYONE.
 */
import { CONFIG } from './config.js';

// Import Firebase SDK via official ESM CDN
import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
import {
  getAuth,
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signOut,
  onAuthStateChanged,
  updateProfile
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
import {
  getFirestore,
  collection,
  doc,
  setDoc,
  getDoc,
  getDocs,
  query,
  orderBy,
  updateDoc,
  deleteDoc
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js';

class FirebaseService {
  constructor() {
    this.app = null;
    this.auth = null;
    this.db = null;
    this.isInitialized = false;
    this.firestoreAvailable = false;
    this.syncChannel = null;
    this.listeners = new Map();
    this.apiBase = (typeof window !== 'undefined' && window.location.origin) ? window.location.origin : 'http://localhost:3000';

    this.init();
  }

  init() {
    // 1. Initialize Firebase if configured
    try {
      if (CONFIG.isLiveFirebase()) {
        this.app = initializeApp(CONFIG.firebase);
        this.auth = getAuth(this.app);
        this.db = getFirestore(this.app);
        this.isInitialized = true;
        console.log('[Aura] Firebase initialized successfully with cloud configuration.');
      }
    } catch (err) {
      console.warn('[Aura] Firebase initialization notice:', err);
    }

    // 2. Set up cross-tab broadcast channel for instant local tab sync
    try {
      this.syncChannel = new BroadcastChannel('aura_sync_channel');
      this.syncChannel.onmessage = (event) => {
        if (event.data?.type && this.listeners.has(event.data.type)) {
          const cbs = this.listeners.get(event.data.type);
          cbs.forEach(cb => cb(event.data.payload));
        }
      };
    } catch (e) {
      console.warn('[Aura] BroadcastChannel notice:', e);
    }

    // 3. Connect to Server-Sent Events (SSE) for Real-Time Cross-Device Sync
    this.initRealtimeStream();

    // 4. Initial Sync across all community data from shared server
    this.syncAllData();

    // 5. Background sync on window focus or visibility change
    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
          this.syncAllData();
        }
      });
    }

    // 6. Periodic heartbeat sync every 6 seconds to ensure total consistency across mobile/desktop
    setInterval(() => {
      this.syncAllData();
    }, 6000);
  }

  // ==========================================
  // SHARED REST API CLIENT HELPERS
  // ==========================================

  async apiGet(endpoint) {
    try {
      const res = await fetch(`${this.apiBase}/api/${endpoint}`, {
        cache: 'no-cache',
        headers: { 'Accept': 'application/json' }
      });
      if (res.ok) {
        return await res.json();
      }
    } catch (err) {
      // Offline or network error
    }
    return null;
  }

  async apiPost(endpoint, data = {}) {
    try {
      const res = await fetch(`${this.apiBase}/api/${endpoint}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(data)
      });
      if (res.ok) {
        return await res.json();
      }
    } catch (err) {
      console.warn(`[Aura API] POST /api/${endpoint} failed:`, err);
    }
    return null;
  }

  async apiDelete(endpoint, options = {}) {
    try {
      const headers = { 'Accept': 'application/json', ...(options.headers || {}) };
      if (options.userId) {
        headers['X-User-Id'] = options.userId;
      }
      const res = await fetch(`${this.apiBase}/api/${endpoint}`, {
        method: 'DELETE',
        headers
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data.error || `Request failed with status ${res.status}`);
      }
      return data;
    } catch (err) {
      console.warn(`[Aura API] DELETE /api/${endpoint} failed:`, err);
      throw err;
    }
  }

  // ==========================================
  // REAL-TIME EVENT STREAM (SSE)
  // ==========================================

  initRealtimeStream() {
    if (typeof EventSource === 'undefined') return;

    try {
      const eventsUrl = `${this.apiBase}/api/events`;
      const es = new EventSource(eventsUrl);

      const handleEvent = (type) => (e) => {
        try {
          const payload = JSON.parse(e.data);
          this.applyIncomingEvent(type, payload);
        } catch (_) {}
      };

      es.addEventListener('POSTS_UPDATED', handleEvent('POSTS_UPDATED'));
      es.addEventListener('REELS_UPDATED', handleEvent('REELS_UPDATED'));
      es.addEventListener('STORIES_UPDATED', handleEvent('STORIES_UPDATED'));
      es.addEventListener('USER_UPDATED', handleEvent('USER_UPDATED'));
      es.addEventListener('FOLLOW_UPDATED', handleEvent('FOLLOW_UPDATED'));
      es.addEventListener('MESSAGE_SENT', handleEvent('MESSAGE_SENT'));
      es.addEventListener('NOTIFICATION_NEW', handleEvent('NOTIFICATION_NEW'));

      es.onerror = () => {
        // Automatically reconnects in browser standard
      };
    } catch (e) {
      console.warn('[Aura] SSE connection notice:', e);
    }
  }

  applyIncomingEvent(type, payload) {
    if (type === 'POSTS_UPDATED' && Array.isArray(payload)) {
      localStorage.setItem('aura_posts', JSON.stringify(payload));
    } else if (type === 'REELS_UPDATED' && Array.isArray(payload)) {
      localStorage.setItem('aura_reels', JSON.stringify(payload));
    } else if (type === 'STORIES_UPDATED' && Array.isArray(payload)) {
      localStorage.setItem('aura_stories', JSON.stringify(payload));
    } else if (type === 'USER_UPDATED' && payload?.uid) {
      const users = this.getLocalUsers();
      const idx = users.findIndex(u => u.uid === payload.uid);
      if (idx >= 0) users[idx] = payload; else users.push(payload);
      localStorage.setItem('aura_users', JSON.stringify(users));

      const cur = this.getLocalCurrentUser();
      if (cur && cur.uid === payload.uid) {
        this.setLocalCurrentUser({ ...cur, ...payload });
      }
    } else if (type === 'MESSAGE_SENT' && payload?.id) {
      const messages = this.getLocalMessages();
      if (!messages.some(m => m.id === payload.id)) {
        messages.push(payload);
        localStorage.setItem('aura_messages', JSON.stringify(messages));
      }
    } else if (type === 'FOLLOW_UPDATED' && payload?.me && payload?.target) {
      const users = this.getLocalUsers();
      [payload.me, payload.target].forEach(updatedUser => {
        const idx = users.findIndex(u => u.uid === updatedUser.uid);
        if (idx >= 0) users[idx] = { ...users[idx], ...updatedUser }; else users.push(updatedUser);
        const cur = this.getLocalCurrentUser();
        if (cur && cur.uid === updatedUser.uid) {
          this.setLocalCurrentUser({ ...cur, ...updatedUser });
        }
      });
      localStorage.setItem('aura_users', JSON.stringify(users));
    } else if (type === 'MESSAGES_UPDATED' && Array.isArray(payload)) {
      localStorage.setItem('aura_messages', JSON.stringify(payload));
    }

    this.emitLocal(type, payload);
    this.broadcast(type, payload);
  }

  // Sync entire dataset from shared backend to keep local caches fresh
  async syncAllData() {
    try {
      const [remotePosts, remoteReels, remoteStories, remoteUsers, remoteMessages] = await Promise.all([
        this.apiGet('posts'),
        this.apiGet('reels'),
        this.apiGet('stories'),
        this.apiGet('users'),
        this.apiGet('messages')
      ]);

      let changed = false;

      if (Array.isArray(remotePosts)) {
        const local = this.getLocalPosts();
        if (JSON.stringify(remotePosts) !== JSON.stringify(local)) {
          localStorage.setItem('aura_posts', JSON.stringify(remotePosts));
          this.emitLocal('POSTS_UPDATED', remotePosts);
          changed = true;
        }
      }

      if (Array.isArray(remoteReels)) {
        const local = this.getLocalReels();
        if (JSON.stringify(remoteReels) !== JSON.stringify(local)) {
          localStorage.setItem('aura_reels', JSON.stringify(remoteReels));
          this.emitLocal('REELS_UPDATED', remoteReels);
          changed = true;
        }
      }

      if (Array.isArray(remoteStories)) {
        const local = this.getLocalStories();
        if (JSON.stringify(remoteStories) !== JSON.stringify(local)) {
          localStorage.setItem('aura_stories', JSON.stringify(remoteStories));
          this.emitLocal('STORIES_UPDATED', remoteStories);
          changed = true;
        }
      }

      if (Array.isArray(remoteUsers)) {
        const local = this.getLocalUsers();
        if (JSON.stringify(remoteUsers) !== JSON.stringify(local)) {
          localStorage.setItem('aura_users', JSON.stringify(remoteUsers));
          this.emitLocal('USERS_UPDATED', remoteUsers);
          changed = true;
        }
      }

      if (Array.isArray(remoteMessages)) {
        const local = this.getLocalMessages();
        if (JSON.stringify(remoteMessages) !== JSON.stringify(local)) {
          localStorage.setItem('aura_messages', JSON.stringify(remoteMessages));
          this.emitLocal('MESSAGES_UPDATED', remoteMessages);
          changed = true;
        }
      }

      return changed;
    } catch (_) {
      return false;
    }
  }

  // Broadcast event across open tabs
  broadcast(type, payload) {
    if (this.syncChannel) {
      try {
        this.syncChannel.postMessage({ type, payload });
      } catch (_) {}
    }
  }

  // Internal subscriber emit
  emitLocal(type, payload) {
    if (this.listeners.has(type)) {
      const cbs = this.listeners.get(type);
      cbs.forEach(cb => {
        try { cb(payload); } catch (err) { console.error(err); }
      });
    }
  }

  // Subscribe to changes
  subscribe(type, callback) {
    if (!this.listeners.has(type)) {
      this.listeners.set(type, new Set());
    }
    this.listeners.get(type).add(callback);
    return () => this.listeners.get(type)?.delete(callback);
  }

  // ==========================================
  // AUTHENTICATION & MULTI-USER PROFILES
  // ==========================================

  onAuthState(callback) {
    if (this.auth) {
      return onAuthStateChanged(this.auth, async (firebaseUser) => {
        if (firebaseUser) {
          const userProfile = await this.getUserProfile(firebaseUser.uid) || {
            uid: firebaseUser.uid,
            email: firebaseUser.email,
            displayName: firebaseUser.displayName || firebaseUser.email.split('@')[0],
            username: (firebaseUser.displayName || firebaseUser.email.split('@')[0]).toLowerCase().replace(/[^a-z0-9_]/g, ''),
            photoURL: firebaseUser.photoURL || `https://api.dicebear.com/7.x/adventurer/svg?seed=${firebaseUser.uid}`,
            bio: '✨ Exploring the Aura community',
            website: '',
            followers: [],
            following: [],
            savedPosts: [],
            createdAt: Date.now()
          };
          this.setLocalCurrentUser(userProfile);
          callback(userProfile);
        } else {
          const localUser = this.getLocalCurrentUser();
          callback(localUser);
        }
      });
    } else {
      const localUser = this.getLocalCurrentUser();
      callback(localUser);
      return () => {};
    }
  }

  async signUp(email, password, username, displayName) {
    const cleanUsername = (username || email.split('@')[0]).toLowerCase().replace(/[^a-z0-9_]/g, '');
    const cleanDisplayName = displayName || username || email.split('@')[0];
    const defaultAvatar = `https://api.dicebear.com/7.x/adventurer/svg?seed=${cleanUsername}`;

    let uid = 'user_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4);
    let authUser = null;

    if (this.auth) {
      try {
        const cred = await createUserWithEmailAndPassword(this.auth, email, password);
        authUser = cred.user;
        uid = authUser.uid;
        await updateProfile(authUser, {
          displayName: cleanDisplayName,
          photoURL: defaultAvatar
        });
      } catch (authErr) {
        // If email already in use or Firebase auth error, fallback gracefully to shared server users
        console.warn('[Aura] Firebase auth notice:', authErr.message);
      }
    }

    const newProfile = {
      uid,
      email,
      username: cleanUsername,
      displayName: cleanDisplayName,
      photoURL: defaultAvatar,
      bio: '✨ Hello Aura! Welcome to my profile.',
      website: '',
      followers: [],
      following: [],
      savedPosts: [],
      createdAt: Date.now()
    };

    // Save globally to shared server database so this user exists across all devices
    await this.saveUserProfile(newProfile);
    this.setLocalCurrentUser(newProfile);
    this.broadcast('USER_UPDATED', newProfile);
    return newProfile;
  }

  async signIn(email, password) {
    // 1. Try Firebase Auth if configured
    if (this.auth) {
      try {
        const cred = await signInWithEmailAndPassword(this.auth, email, password);
        let profile = await this.getUserProfile(cred.user.uid);
        if (!profile) {
          profile = {
            uid: cred.user.uid,
            email: cred.user.email,
            username: (cred.user.displayName || cred.user.email.split('@')[0]).toLowerCase().replace(/[^a-z0-9_]/g, ''),
            displayName: cred.user.displayName || cred.user.email.split('@')[0],
            photoURL: cred.user.photoURL || `https://api.dicebear.com/7.x/adventurer/svg?seed=${cred.user.uid}`,
            bio: '✨ Living in the moment',
            website: '',
            followers: [],
            following: [],
            savedPosts: [],
            createdAt: Date.now()
          };
          await this.saveUserProfile(profile);
        }
        this.setLocalCurrentUser(profile);
        return profile;
      } catch (authErr) {
        console.warn('[Aura] Firebase signIn notice:', authErr.message);
      }
    }

    // 2. Check globally shared users database
    let users = await this.apiGet('users');
    if (!Array.isArray(users) || users.length === 0) {
      users = this.getLocalUsers();
    }

    const match = users.find(u => u.email === email || u.username === email);
    if (match) {
      this.setLocalCurrentUser(match);
      return match;
    }

    throw new Error('Account not found. Please check your credentials or sign up.');
  }

  async signOut() {
    if (this.auth) {
      try { await signOut(this.auth); } catch (_) {}
    }
    this.setLocalCurrentUser(null);
    this.broadcast('USER_LOGOUT', null);
  }

  // ==========================================
  // USER PROFILES
  // ==========================================

  async getUserProfile(uid) {
    if (!uid) return null;

    // 1. Check shared server database
    const remoteUser = await this.apiGet(`users/${uid}`);
    if (remoteUser && remoteUser.uid) {
      return remoteUser;
    }

    // 2. Check Firestore if live
    if (this.db) {
      try {
        const docRef = doc(this.db, 'users', uid);
        const snap = await getDoc(docRef);
        if (snap.exists()) {
          return { uid, ...snap.data() };
        }
      } catch (_) {}
    }

    // 3. Check local users
    const users = this.getLocalUsers();
    return users.find(u => u.uid === uid || u.username === uid) || null;
  }

  async saveUserProfile(profile) {
    if (!profile || !profile.uid) return;

    // 1. Persist to shared backend server
    await this.apiPost('users', profile);

    // 2. Persist to Firestore if available
    if (this.db) {
      try {
        await setDoc(doc(this.db, 'users', profile.uid), profile, { merge: true });
      } catch (_) {}
    }

    // 3. Update local cache
    const users = this.getLocalUsers();
    const idx = users.findIndex(u => u.uid === profile.uid);
    if (idx >= 0) {
      users[idx] = { ...users[idx], ...profile };
    } else {
      users.push(profile);
    }
    localStorage.setItem('aura_users', JSON.stringify(users));

    const cur = this.getLocalCurrentUser();
    if (cur && cur.uid === profile.uid) {
      this.setLocalCurrentUser({ ...cur, ...profile });
    }

    this.broadcast('USER_UPDATED', profile);
  }

  async updateUserProfile(uid, updates) {
    const existing = await this.getUserProfile(uid) || {};
    const updated = { ...existing, ...updates, uid, updatedAt: Date.now() };
    await this.saveUserProfile(updated);

    if (this.auth?.currentUser && this.auth.currentUser.uid === uid) {
      try {
        await updateProfile(this.auth.currentUser, {
          displayName: updated.displayName,
          photoURL: updated.photoURL
        });
      } catch (_) {}
    }

    this.broadcast('USER_UPDATED', updated);
    return updated;
  }

  // ==========================================
  // POSTS (SHARED GLOBALLY ACROSS ALL ACCOUNTS)
  // ==========================================

  async createPost(postData) {
    const post = {
      id: 'post_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
      likes: [],
      comments: [],
      savedBy: [],
      createdAt: Date.now(),
      ...postData
    };

    // 1. Persist to shared server backend so ALL devices & accounts see it immediately
    const serverSaved = await this.apiPost('posts', post);
    const finalPost = serverSaved || post;

    // 2. Persist to Firestore if available
    if (this.db) {
      try {
        await setDoc(doc(this.db, 'posts', finalPost.id), finalPost);
      } catch (_) {}
    }

    // 3. Update local cache
    const posts = this.getLocalPosts();
    posts.unshift(finalPost);
    localStorage.setItem('aura_posts', JSON.stringify(posts));

    this.broadcast('POSTS_UPDATED', posts);
    return finalPost;
  }

  async getPosts() {
    // 1. Always fetch newest shared posts from backend server
    const remotePosts = await this.apiGet('posts');
    if (Array.isArray(remotePosts) && remotePosts.length > 0) {
      localStorage.setItem('aura_posts', JSON.stringify(remotePosts));
      return remotePosts;
    }

    // 2. Try Firestore fallback
    if (this.db) {
      try {
        const postsRef = collection(this.db, 'posts');
        const q = query(postsRef, orderBy('createdAt', 'desc'));
        const snap = await getDocs(q);
        if (!snap.empty) {
          const list = snap.docs.map(d => ({ id: d.id, ...d.data() }));
          localStorage.setItem('aura_posts', JSON.stringify(list));
          return list;
        }
      } catch (_) {}
    }

    // 3. Fallback to local cache
    return this.getLocalPosts();
  }

  async deletePost(postId, userId = null) {
    if (!userId) {
      if (this.auth?.currentUser?.uid) {
        userId = this.auth.currentUser.uid;
      } else {
        try {
          const stored = JSON.parse(localStorage.getItem('aura_user') || '{}');
          if (stored && stored.uid) userId = stored.uid;
        } catch (_) {}
      }
    }
    if (!userId) {
      throw new Error('Authentication required to delete post');
    }

    // Verify ownership against cached post
    const cachedPost = this.getLocalPosts().find(p => p.id === postId);
    if (cachedPost && cachedPost.userId && cachedPost.userId !== userId) {
      throw new Error("Forbidden: You cannot delete another user's post");
    }

    // Call API with authorization
    await this.apiDelete(`posts/${postId}?userId=${encodeURIComponent(userId)}`, { userId });

    if (this.db) {
      try { await deleteDoc(doc(this.db, 'posts', postId)); } catch (_) {}
    }
    const posts = this.getLocalPosts().filter(p => p.id !== postId);
    localStorage.setItem('aura_posts', JSON.stringify(posts));
    this.emitLocal('POSTS_UPDATED', posts);
    this.broadcast('POSTS_UPDATED', posts);
    return true;
  }

  async toggleLikePost(postId, userId) {
    // 1. Send like toggle to shared backend server
    const result = await this.apiPost(`posts/${postId}/like`, { userId });

    const posts = this.getLocalPosts();
    const post = posts.find(p => p.id === postId);
    if (!post) return result;

    if (!Array.isArray(post.likes)) post.likes = [];
    const idx = post.likes.indexOf(userId);
    let liked = false;
    if (idx >= 0) {
      post.likes.splice(idx, 1);
      liked = false;
    } else {
      post.likes.push(userId);
      liked = true;
    }

    if (this.db) {
      try {
        await updateDoc(doc(this.db, 'posts', postId), { likes: post.likes });
      } catch (_) {}
    }

    localStorage.setItem('aura_posts', JSON.stringify(posts));
    this.broadcast('POSTS_UPDATED', posts);
    return result || { liked, count: post.likes.length, post };
  }

  async addComment(postId, commentData) {
    const commentPayload = {
      id: 'cmt_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
      createdAt: Date.now(),
      likes: [],
      ...commentData
    };

    // 1. Persist comment to shared server
    const serverComment = await this.apiPost(`posts/${postId}/comment`, commentPayload);
    const newComment = serverComment || commentPayload;

    // 2. Update local post cache
    const posts = this.getLocalPosts();
    const post = posts.find(p => p.id === postId);
    if (post) {
      if (!Array.isArray(post.comments)) post.comments = [];
      post.comments.push(newComment);
      localStorage.setItem('aura_posts', JSON.stringify(posts));

      if (this.db) {
        try {
          await updateDoc(doc(this.db, 'posts', postId), { comments: post.comments });
        } catch (_) {}
      }
    }

    this.broadcast('POSTS_UPDATED', posts);
    return newComment;
  }

  async toggleSavePost(postId, userId) {
    const user = await this.getUserProfile(userId);
    if (!user) return false;

    if (!Array.isArray(user.savedPosts)) user.savedPosts = [];
    const idx = user.savedPosts.indexOf(postId);
    let saved = false;
    if (idx >= 0) {
      user.savedPosts.splice(idx, 1);
      saved = false;
    } else {
      user.savedPosts.push(postId);
      saved = true;
    }

    await this.saveUserProfile(user);
    this.broadcast('USER_UPDATED', user);
    return saved;
  }

  // ==========================================
  // REELS (SHARED GLOBALLY ACROSS ALL ACCOUNTS)
  // ==========================================

  async createReel(reelData) {
    const reel = {
      id: 'reel_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
      likes: [],
      comments: [],
      shares: 0,
      createdAt: Date.now(),
      ...reelData
    };

    // 1. Persist to shared server backend so ALL devices & accounts see it immediately
    const serverSaved = await this.apiPost('reels', reel);
    const finalReel = serverSaved || reel;

    // 2. Persist to Firestore if available
    if (this.db) {
      try {
        await setDoc(doc(this.db, 'reels', finalReel.id), finalReel);
      } catch (_) {}
    }

    // 3. Update local cache
    const reels = this.getLocalReels();
    reels.unshift(finalReel);
    localStorage.setItem('aura_reels', JSON.stringify(reels));

    this.broadcast('REELS_UPDATED', reels);
    return finalReel;
  }

  async getReels() {
    // 1. Fetch newest shared reels from backend server
    const remoteReels = await this.apiGet('reels');
    if (Array.isArray(remoteReels) && remoteReels.length > 0) {
      localStorage.setItem('aura_reels', JSON.stringify(remoteReels));
      return remoteReels;
    }

    // 2. Try Firestore fallback
    if (this.db) {
      try {
        const reelsRef = collection(this.db, 'reels');
        const q = query(reelsRef, orderBy('createdAt', 'desc'));
        const snap = await getDocs(q);
        if (!snap.empty) {
          const list = snap.docs.map(d => ({ id: d.id, ...d.data() }));
          localStorage.setItem('aura_reels', JSON.stringify(list));
          return list;
        }
      } catch (_) {}
    }

    // 3. Fallback to local cache
    return this.getLocalReels();
  }

  async deleteReel(reelId, userId = null) {
    if (!userId) {
      if (this.auth?.currentUser?.uid) {
        userId = this.auth.currentUser.uid;
      } else {
        try {
          const stored = JSON.parse(localStorage.getItem('aura_user') || '{}');
          if (stored && stored.uid) userId = stored.uid;
        } catch (_) {}
      }
    }
    if (!userId) {
      throw new Error('Authentication required to delete reel');
    }

    // Verify ownership against cached reel
    const cachedReel = this.getLocalReels().find(r => r.id === reelId);
    if (cachedReel && cachedReel.userId && cachedReel.userId !== userId) {
      throw new Error("Forbidden: You cannot delete another user's reel");
    }

    // Call API with authorization
    await this.apiDelete(`reels/${reelId}?userId=${encodeURIComponent(userId)}`, { userId });

    if (this.db) {
      try { await deleteDoc(doc(this.db, 'reels', reelId)); } catch (_) {}
    }
    const reels = this.getLocalReels().filter(r => r.id !== reelId);
    localStorage.setItem('aura_reels', JSON.stringify(reels));
    this.emitLocal('REELS_UPDATED', reels);
    this.broadcast('REELS_UPDATED', reels);
    return true;
  }

  async toggleLikeReel(reelId, userId) {
    const result = await this.apiPost(`reels/${reelId}/like`, { userId });

    const reels = this.getLocalReels();
    const reel = reels.find(r => r.id === reelId);
    if (!reel) return result;

    if (!Array.isArray(reel.likes)) reel.likes = [];
    const idx = reel.likes.indexOf(userId);
    let liked = false;
    if (idx >= 0) {
      reel.likes.splice(idx, 1);
      liked = false;
    } else {
      reel.likes.push(userId);
      liked = true;
    }

    if (this.db) {
      try {
        await updateDoc(doc(this.db, 'reels', reelId), { likes: reel.likes });
      } catch (_) {}
    }

    localStorage.setItem('aura_reels', JSON.stringify(reels));
    this.broadcast('REELS_UPDATED', reels);
    return result || { liked, count: reel.likes.length, reel };
  }

  async addReelComment(reelId, commentData) {
    const commentPayload = {
      id: 'cmt_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
      createdAt: Date.now(),
      likes: [],
      ...commentData
    };

    const serverComment = await this.apiPost(`reels/${reelId}/comment`, commentPayload);
    const newComment = serverComment || commentPayload;

    const reels = this.getLocalReels();
    const reel = reels.find(r => r.id === reelId);
    if (reel) {
      if (!Array.isArray(reel.comments)) reel.comments = [];
      reel.comments.push(newComment);
      localStorage.setItem('aura_reels', JSON.stringify(reels));
    }

    this.broadcast('REELS_UPDATED', reels);
    return newComment;
  }

  // ==========================================
  // STORIES (SHARED GLOBALLY ACROSS ALL ACCOUNTS)
  // ==========================================

  async createStory(storyData) {
    const story = {
      id: 'story_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
      viewers: [],
      reactions: [],
      createdAt: Date.now(),
      expiresAt: Date.now() + 24 * 60 * 60 * 1000,
      ...storyData
    };

    // 1. Persist to shared backend server
    const serverSaved = await this.apiPost('stories', story);
    const finalStory = serverSaved || story;

    // 2. Persist to Firestore if available
    if (this.db) {
      try {
        await setDoc(doc(this.db, 'stories', finalStory.id), finalStory);
      } catch (_) {}
    }

    // 3. Update local cache
    const stories = this.getLocalStories();
    stories.push(finalStory);
    localStorage.setItem('aura_stories', JSON.stringify(stories));

    this.broadcast('STORIES_UPDATED', stories);
    return finalStory;
  }

  async getActiveStories() {
    const remoteStories = await this.apiGet('stories');
    if (Array.isArray(remoteStories) && remoteStories.length > 0) {
      localStorage.setItem('aura_stories', JSON.stringify(remoteStories));
      return remoteStories;
    }

    const now = Date.now();
    const all = this.getLocalStories();
    return all.filter(s => (s.expiresAt || 0) > now);
  }

  async markStoryViewed(storyId, userId) {
    await this.apiPost(`stories/${storyId}/view`, { userId });

    const stories = this.getLocalStories();
    const story = stories.find(s => s.id === storyId);
    if (!story) return;

    if (!Array.isArray(story.viewers)) story.viewers = [];
    if (!story.viewers.includes(userId)) {
      story.viewers.push(userId);
      localStorage.setItem('aura_stories', JSON.stringify(stories));
      this.broadcast('STORIES_UPDATED', stories);
    }
  }

  async deleteStory(storyId, userId) {
    await this.apiDelete(`stories/${storyId}`);
    if (this.db) {
      try { await deleteDoc(doc(this.db, 'stories', storyId)); } catch (_) {}
    }
    const stories = this.getLocalStories().filter(s => !(s.id === storyId && s.userId === userId));
    localStorage.setItem('aura_stories', JSON.stringify(stories));
    this.broadcast('STORIES_UPDATED', stories);
    return true;
  }

  // ==========================================
  // FOLLOWERS / FOLLOWING
  // ==========================================

  async toggleFollow(currentUserId, targetUserId) {
    if (!currentUserId || !targetUserId || currentUserId === targetUserId) return false;

    // 1. Send follow toggle request to backend server
    let res = null;
    try {
      res = await this.apiPost(`users/${targetUserId}/follow`, { followerId: currentUserId });
    } catch (e) {
      console.warn('[Aura Service] Backend follow toggle fallback:', e);
    }

    const users = this.getLocalUsers();
    let me = users.find(u => u.uid === currentUserId) || this.getLocalCurrentUser() || { uid: currentUserId };
    let target = users.find(u => u.uid === targetUserId) || { uid: targetUserId };

    if (!Array.isArray(me.following)) me.following = [];
    if (!Array.isArray(target.followers)) target.followers = [];

    // Determine target follow status:
    // If backend returned follow status, respect res.following; otherwise toggle locally
    let shouldBeFollowing;
    if (res && typeof res.following === 'boolean') {
      shouldBeFollowing = res.following;
    } else {
      shouldBeFollowing = !me.following.includes(targetUserId);
    }

    if (shouldBeFollowing) {
      if (!me.following.includes(targetUserId)) me.following.push(targetUserId);
      if (!target.followers.includes(currentUserId)) target.followers.push(currentUserId);

      await this.createNotification({
        recipientId: targetUserId,
        senderId: currentUserId,
        type: 'follow',
        text: 'started following you.',
        timestamp: Date.now()
      });
    } else {
      me.following = me.following.filter(id => id !== targetUserId);
      target.followers = target.followers.filter(id => id !== currentUserId);
    }

    // Persist to local users cache
    const meIdx = users.findIndex(u => u.uid === currentUserId);
    if (meIdx >= 0) users[meIdx] = { ...users[meIdx], ...me }; else users.push(me);

    const targetIdx = users.findIndex(u => u.uid === targetUserId);
    if (targetIdx >= 0) users[targetIdx] = { ...users[targetIdx], ...target }; else users.push(target);

    localStorage.setItem('aura_users', JSON.stringify(users));

    // Update current user cache in localStorage
    const cur = this.getLocalCurrentUser();
    if (cur && cur.uid === currentUserId) {
      this.setLocalCurrentUser({ ...cur, following: me.following });
    }

    // Update Firestore if available
    if (this.db) {
      try {
        await updateDoc(doc(this.db, 'users', currentUserId), { following: me.following });
        await updateDoc(doc(this.db, 'users', targetUserId), { followers: target.followers });
      } catch (_) {}
    }

    this.emitLocal('FOLLOW_UPDATED', { me, target });
    this.broadcast('FOLLOW_UPDATED', { me, target });

    return shouldBeFollowing;
  }

  // ==========================================
  // DIRECT MESSAGING (DMs)
  // ==========================================

  getChatId(uid1, uid2) {
    return [uid1, uid2].sort().join('_chat_');
  }

  async sendMessage(senderId, recipientId, content, mediaUrl = null) {
    const chatId = this.getChatId(senderId, recipientId);
    const message = {
      id: 'msg_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5),
      chatId,
      senderId,
      recipientId,
      content,
      mediaUrl,
      timestamp: Date.now(),
      read: false
    };

    // 1. Post to shared backend
    const serverSaved = await this.apiPost('messages', message);
    const finalMsg = serverSaved || message;

    // 2. Firestore if available
    if (this.db) {
      try {
        await setDoc(doc(this.db, 'messages', finalMsg.id), finalMsg);
      } catch (_) {}
    }

    // 3. Local cache
    const messages = this.getLocalMessages();
    messages.push(finalMsg);
    localStorage.setItem('aura_messages', JSON.stringify(messages));
    this.broadcast('MESSAGE_SENT', finalMsg);
    return finalMsg;
  }

  async fetchMessages() {
    try {
      const remote = await this.apiGet('messages');
      if (Array.isArray(remote)) {
        localStorage.setItem('aura_messages', JSON.stringify(remote));
        this.emitLocal('MESSAGES_UPDATED', remote);
        return remote;
      }
    } catch (_) {}
    return this.getLocalMessages();
  }

  getMessages(uid1, uid2) {
    const chatId = this.getChatId(uid1, uid2);
    const all = this.getLocalMessages();
    return all.filter(m => m.chatId === chatId).sort((a, b) => a.timestamp - b.timestamp);
  }

  getRecentConversations(currentUserId) {
    const all = this.getLocalMessages();
    const chats = new Map();

    all.forEach(m => {
      if (m.senderId === currentUserId || m.recipientId === currentUserId) {
        const partnerId = m.senderId === currentUserId ? m.recipientId : m.senderId;
        const existing = chats.get(partnerId);
        if (!existing || m.timestamp > existing.timestamp) {
          chats.set(partnerId, m);
        }
      }
    });

    return Array.from(chats.entries()).map(([partnerId, lastMessage]) => ({
      partnerId,
      lastMessage
    })).sort((a, b) => b.lastMessage.timestamp - a.lastMessage.timestamp);
  }

  // ==========================================
  // NOTIFICATIONS
  // ==========================================

  async createNotification(notifData) {
    const notif = {
      id: 'notif_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
      read: false,
      timestamp: Date.now(),
      ...notifData
    };

    await this.apiPost('notifications', notif);

    const list = this.getLocalNotifications();
    list.unshift(notif);
    localStorage.setItem('aura_notifications', JSON.stringify(list));
    this.broadcast('NOTIFICATION_NEW', notif);
    return notif;
  }

  getNotifications(userId) {
    return this.getLocalNotifications().filter(n => n.recipientId === userId);
  }

  // ==========================================
  // LOCAL STORAGE HELPERS
  // ==========================================

  getLocalCurrentUser() {
    try {
      const data = localStorage.getItem('aura_current_user');
      return data ? JSON.parse(data) : null;
    } catch (_) {
      return null;
    }
  }

  setLocalCurrentUser(user) {
    if (user) {
      localStorage.setItem('aura_current_user', JSON.stringify(user));
    } else {
      localStorage.removeItem('aura_current_user');
    }
  }

  getLocalUsers() {
    try {
      return JSON.parse(localStorage.getItem('aura_users') || '[]');
    } catch (_) {
      return [];
    }
  }

  getLocalPosts() {
    try {
      return JSON.parse(localStorage.getItem('aura_posts') || '[]');
    } catch (_) {
      return [];
    }
  }

  getLocalReels() {
    try {
      return JSON.parse(localStorage.getItem('aura_reels') || '[]');
    } catch (_) {
      return [];
    }
  }

  getLocalStories() {
    try {
      return JSON.parse(localStorage.getItem('aura_stories') || '[]');
    } catch (_) {
      return [];
    }
  }

  getLocalMessages() {
    try {
      return JSON.parse(localStorage.getItem('aura_messages') || '[]');
    } catch (_) {
      return [];
    }
  }

  getLocalNotifications() {
    try {
      return JSON.parse(localStorage.getItem('aura_notifications') || '[]');
    } catch (_) {
      return [];
    }
  }
}

export const firebaseService = new FirebaseService();
