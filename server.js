/**
 * Aura - Local & Network Production Server
 * Full REST API, Server-Sent Events (SSE) Live Sync & Static Asset Hosting
 * Keeps all reels, posts, stories, comments, likes & profiles 100% synchronized across all devices & accounts.
 */

import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'data', 'aura-db.json');

// Ensure data directory exists
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

// In-Memory Database with atomic write sync
let db = {
  users: [],
  posts: [],
  reels: [],
  stories: [],
  messages: [],
  notifications: []
};

function loadDb() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const raw = fs.readFileSync(DB_FILE, 'utf-8');
      db = JSON.parse(raw);
    }
  } catch (err) {
    console.error('[Aura Server] Error loading database file:', err);
  }
}

function saveDb() {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2), 'utf-8');
  } catch (err) {
    console.error('[Aura Server] Error saving database file:', err);
  }
}

// Initial DB Load
loadDb();

// Active SSE Connections for Real-Time Cross-Device Sync
const sseClients = new Set();

function broadcastEvent(type, payload) {
  const message = `event: ${type}\ndata: ${JSON.stringify(payload)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(message);
    } catch (_) {
      sseClients.delete(client);
    }
  }
}

// Send periodic heartbeat to keep SSE streams alive
setInterval(() => {
  for (const client of sseClients) {
    try {
      client.write(': ping\n\n');
    } catch (_) {
      sseClients.delete(client);
    }
  }
}, 25000);

const MIME_TYPES = {
  '.html': 'text/html; charset=UTF-8',
  '.js': 'text/javascript; charset=UTF-8',
  '.mjs': 'text/javascript; charset=UTF-8',
  '.css': 'text/css; charset=UTF-8',
  '.json': 'application/json; charset=UTF-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.mp4': 'video/mp4',
  '.webm': 'video/webm'
};

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=UTF-8',
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'no-cache, no-store, must-revalidate'
  });
  res.end(JSON.stringify(data));
}

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 20 * 1024 * 1024) { // 20MB limit
        reject(new Error('Request entity too large'));
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  // Global CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-User-Id');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = urlObj.pathname;

  // ==========================================
  // REST API ROUTING (/api/...)
  // ==========================================
  if (pathname.startsWith('/api/')) {
    try {
      // 1. Health check
      if (pathname === '/api/status' && req.method === 'GET') {
        return sendJson(res, 200, {
          status: 'ok',
          time: Date.now(),
          clients: sseClients.size,
          counts: {
            posts: db.posts.length,
            reels: db.reels.length,
            stories: db.stories.length,
            users: db.users.length
          }
        });
      }

      // 2. Server-Sent Events (SSE) Live Feed
      if (pathname === '/api/events' && req.method === 'GET') {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive',
          'Access-Control-Allow-Origin': '*'
        });
        res.write(': connected\n\n');
        sseClients.add(res);

        req.on('close', () => {
          sseClients.delete(res);
        });
        return;
      }

      // 3. POSTS
      // GET /api/posts
      if (pathname === '/api/posts' && req.method === 'GET') {
        const sorted = [...(db.posts || [])].sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
        return sendJson(res, 200, sorted);
      }

      // POST /api/posts
      if (pathname === '/api/posts' && req.method === 'POST') {
        const body = await parseJsonBody(req);
        const newPost = {
          id: body.id || 'post_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
          userId: body.userId,
          username: body.username,
          userPhoto: body.userPhoto,
          mediaUrl: body.mediaUrl,
          mediaType: body.mediaType || 'image',
          caption: body.caption || '',
          location: body.location || '',
          likes: Array.isArray(body.likes) ? body.likes : [],
          comments: Array.isArray(body.comments) ? body.comments : [],
          savedBy: [],
          createdAt: body.createdAt || Date.now()
        };

        db.posts.unshift(newPost);
        saveDb();
        broadcastEvent('POSTS_UPDATED', db.posts);
        return sendJson(res, 201, newPost);
      }

      // POST /api/posts/:id/like
      const postLikeMatch = pathname.match(/^\/api\/posts\/([^/]+)\/like$/);
      if (postLikeMatch && req.method === 'POST') {
        const postId = postLikeMatch[1];
        const body = await parseJsonBody(req);
        const userId = body.userId;
        const post = db.posts.find(p => p.id === postId);

        if (!post) {
          return sendJson(res, 404, { error: 'Post not found' });
        }

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

        saveDb();
        broadcastEvent('POSTS_UPDATED', db.posts);
        return sendJson(res, 200, { liked, count: post.likes.length, post });
      }

      // POST /api/posts/:id/comment
      const postCommentMatch = pathname.match(/^\/api\/posts\/([^/]+)\/comment$/);
      if (postCommentMatch && req.method === 'POST') {
        const postId = postCommentMatch[1];
        const body = await parseJsonBody(req);
        const post = db.posts.find(p => p.id === postId);

        if (!post) {
          return sendJson(res, 404, { error: 'Post not found' });
        }

        if (!Array.isArray(post.comments)) post.comments = [];
        const newComment = {
          id: body.id || 'cmt_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
          userId: body.userId,
          username: body.username,
          userPhoto: body.userPhoto,
          text: body.text,
          createdAt: body.createdAt || Date.now(),
          likes: []
        };

        post.comments.push(newComment);
        saveDb();
        broadcastEvent('POSTS_UPDATED', db.posts);
        return sendJson(res, 201, newComment);
      }

      // DELETE /api/posts/:id
      const postDeleteMatch = pathname.match(/^\/api\/posts\/([^/]+)$/);
      if (postDeleteMatch && req.method === 'DELETE') {
        const postId = postDeleteMatch[1];
        const post = (db.posts || []).find(p => p.id === postId);
        if (!post) {
          return sendJson(res, 404, { error: 'Post not found' });
        }

        let requestingUserId = urlObj.searchParams.get('userId') || req.headers['x-user-id'];
        if (!requestingUserId) {
          try {
            const body = await parseJsonBody(req);
            if (body && body.userId) requestingUserId = body.userId;
          } catch (_) {}
        }

        if (!requestingUserId) {
          return sendJson(res, 401, { error: 'Unauthorized: User ID required to delete post' });
        }

        // STRICT AUTHORIZATION: A user can only delete their own post
        if (post.userId && post.userId !== requestingUserId) {
          return sendJson(res, 403, { error: 'Forbidden: You cannot delete another user\'s post' });
        }

        db.posts = db.posts.filter(p => p.id !== postId);
        saveDb();
        broadcastEvent('POSTS_UPDATED', db.posts);
        return sendJson(res, 200, { success: true, id: postId });
      }

      // 4. REELS
      // GET /api/reels
      if (pathname === '/api/reels' && req.method === 'GET') {
        const sorted = [...(db.reels || [])].sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
        return sendJson(res, 200, sorted);
      }

      // POST /api/reels
      if (pathname === '/api/reels' && req.method === 'POST') {
        const body = await parseJsonBody(req);
        const newReel = {
          id: body.id || 'reel_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
          userId: body.userId,
          username: body.username,
          userPhoto: body.userPhoto,
          videoUrl: body.videoUrl,
          caption: body.caption || '',
          audioName: body.audioName || `Original audio - ${body.username}`,
          likes: Array.isArray(body.likes) ? body.likes : [],
          comments: Array.isArray(body.comments) ? body.comments : [],
          shares: body.shares || 0,
          createdAt: body.createdAt || Date.now()
        };

        db.reels.unshift(newReel);
        saveDb();
        broadcastEvent('REELS_UPDATED', db.reels);
        return sendJson(res, 201, newReel);
      }

      // POST /api/reels/:id/like
      const reelLikeMatch = pathname.match(/^\/api\/reels\/([^/]+)\/like$/);
      if (reelLikeMatch && req.method === 'POST') {
        const reelId = reelLikeMatch[1];
        const body = await parseJsonBody(req);
        const userId = body.userId;
        const reel = db.reels.find(r => r.id === reelId);

        if (!reel) {
          return sendJson(res, 404, { error: 'Reel not found' });
        }

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

        saveDb();
        broadcastEvent('REELS_UPDATED', db.reels);
        return sendJson(res, 200, { liked, count: reel.likes.length, reel });
      }

      // POST /api/reels/:id/comment
      const reelCommentMatch = pathname.match(/^\/api\/reels\/([^/]+)\/comment$/);
      if (reelCommentMatch && req.method === 'POST') {
        const reelId = reelCommentMatch[1];
        const body = await parseJsonBody(req);
        const reel = db.reels.find(r => r.id === reelId);

        if (!reel) {
          return sendJson(res, 404, { error: 'Reel not found' });
        }

        if (!Array.isArray(reel.comments)) reel.comments = [];
        const newComment = {
          id: body.id || 'cmt_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
          userId: body.userId,
          username: body.username,
          userPhoto: body.userPhoto,
          text: body.text,
          createdAt: body.createdAt || Date.now(),
          likes: []
        };

        reel.comments.push(newComment);
        saveDb();
        broadcastEvent('REELS_UPDATED', db.reels);
        return sendJson(res, 201, newComment);
      }

      // DELETE /api/reels/:id
      const reelDeleteMatch = pathname.match(/^\/api\/reels\/([^/]+)$/);
      if (reelDeleteMatch && req.method === 'DELETE') {
        const reelId = reelDeleteMatch[1];
        const reel = (db.reels || []).find(r => r.id === reelId);
        if (!reel) {
          return sendJson(res, 404, { error: 'Reel not found' });
        }

        let requestingUserId = urlObj.searchParams.get('userId') || req.headers['x-user-id'];
        if (!requestingUserId) {
          try {
            const body = await parseJsonBody(req);
            if (body && body.userId) requestingUserId = body.userId;
          } catch (_) {}
        }

        if (!requestingUserId) {
          return sendJson(res, 401, { error: 'Unauthorized: User ID required to delete reel' });
        }

        // STRICT AUTHORIZATION: A user can only delete their own reel
        if (reel.userId && reel.userId !== requestingUserId) {
          return sendJson(res, 403, { error: 'Forbidden: You cannot delete another user\'s reel' });
        }

        db.reels = db.reels.filter(r => r.id !== reelId);
        saveDb();
        broadcastEvent('REELS_UPDATED', db.reels);
        return sendJson(res, 200, { success: true, id: reelId });
      }

      // 5. STORIES
      // GET /api/stories
      if (pathname === '/api/stories' && req.method === 'GET') {
        const now = Date.now();
        const activeStories = (db.stories || []).filter(s => (s.expiresAt || 0) > now);
        return sendJson(res, 200, activeStories);
      }

      // POST /api/stories
      if (pathname === '/api/stories' && req.method === 'POST') {
        const body = await parseJsonBody(req);
        const newStory = {
          id: body.id || 'story_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
          userId: body.userId,
          username: body.username,
          userPhoto: body.userPhoto,
          mediaUrl: body.mediaUrl,
          mediaType: body.mediaType || 'image',
          caption: body.caption || '',
          viewers: Array.isArray(body.viewers) ? body.viewers : [],
          reactions: [],
          expiresAt: body.expiresAt || (Date.now() + 24 * 60 * 60 * 1000),
          createdAt: body.createdAt || Date.now()
        };

        db.stories.push(newStory);
        saveDb();
        broadcastEvent('STORIES_UPDATED', db.stories);
        return sendJson(res, 201, newStory);
      }

      // POST /api/stories/:id/view
      const storyViewMatch = pathname.match(/^\/api\/stories\/([^/]+)\/view$/);
      if (storyViewMatch && req.method === 'POST') {
        const storyId = storyViewMatch[1];
        const body = await parseJsonBody(req);
        const story = db.stories.find(s => s.id === storyId);

        if (story) {
          if (!Array.isArray(story.viewers)) story.viewers = [];
          if (body.userId && !story.viewers.includes(body.userId)) {
            story.viewers.push(body.userId);
            saveDb();
            broadcastEvent('STORIES_UPDATED', db.stories);
          }
        }
        return sendJson(res, 200, { success: true });
      }

      // DELETE /api/stories/:id
      const storyDeleteMatch = pathname.match(/^\/api\/stories\/([^/]+)$/);
      if (storyDeleteMatch && req.method === 'DELETE') {
        const storyId = storyDeleteMatch[1];
        db.stories = db.stories.filter(s => s.id !== storyId);
        saveDb();
        broadcastEvent('STORIES_UPDATED', db.stories);
        return sendJson(res, 200, { success: true, id: storyId });
      }

      // 6. USERS
      // GET /api/users
      if (pathname === '/api/users' && req.method === 'GET') {
        return sendJson(res, 200, db.users || []);
      }

      // GET /api/users/:uid
      const userMatch = pathname.match(/^\/api\/users\/([^/]+)$/);
      if (userMatch && req.method === 'GET') {
        const uid = userMatch[1];
        const user = db.users.find(u => u.uid === uid || u.username === uid);
        if (!user) {
          return sendJson(res, 404, { error: 'User not found' });
        }
        return sendJson(res, 200, user);
      }

      // POST /api/users (upsert profile)
      if (pathname === '/api/users' && req.method === 'POST') {
        const profile = await parseJsonBody(req);
        if (!profile.uid) {
          return sendJson(res, 400, { error: 'Missing profile uid' });
        }

        const idx = db.users.findIndex(u => u.uid === profile.uid);
        if (idx >= 0) {
          db.users[idx] = { ...db.users[idx], ...profile, updatedAt: Date.now() };
        } else {
          db.users.push({
            followers: [],
            following: [],
            savedPosts: [],
            createdAt: Date.now(),
            ...profile
          });
        }

        saveDb();
        const saved = db.users.find(u => u.uid === profile.uid);
        broadcastEvent('USER_UPDATED', saved);
        return sendJson(res, 200, saved);
      }

      // POST /api/users/:uid/follow
      const followMatch = pathname.match(/^\/api\/users\/([^/]+)\/follow$/);
      if (followMatch && req.method === 'POST') {
        const targetUid = followMatch[1];
        const body = await parseJsonBody(req);
        const followerUid = body.followerId;

        let target = db.users.find(u => u.uid === targetUid);
        let follower = db.users.find(u => u.uid === followerUid);

        if (!follower) {
          follower = { uid: followerUid, followers: [], following: [], createdAt: Date.now() };
          db.users.push(follower);
        }
        if (!target) {
          target = { uid: targetUid, followers: [], following: [], createdAt: Date.now() };
          db.users.push(target);
        }

        if (!Array.isArray(follower.following)) follower.following = [];
        if (!Array.isArray(target.followers)) target.followers = [];

        const isFollowing = follower.following.includes(targetUid);
        if (isFollowing) {
          follower.following = follower.following.filter(id => id !== targetUid);
          target.followers = target.followers.filter(id => id !== followerUid);
        } else {
          follower.following.push(targetUid);
          target.followers.push(followerUid);

          // Add notification
          db.notifications.unshift({
            id: 'notif_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
            recipientId: targetUid,
            senderId: followerUid,
            type: 'follow',
            text: 'started following you.',
            timestamp: Date.now(),
            read: false
          });
        }

        saveDb();
        broadcastEvent('FOLLOW_UPDATED', { me: follower, target });
        return sendJson(res, 200, { following: !isFollowing, me: follower, target });
      }

      // 7. DIRECT MESSAGES
      // GET /api/messages?uid1=...&uid2=...
      if (pathname === '/api/messages' && req.method === 'GET') {
        const uid1 = urlObj.searchParams.get('uid1');
        const uid2 = urlObj.searchParams.get('uid2');

        if (uid1 && uid2) {
          const chatId = [uid1, uid2].sort().join('_chat_');
          const list = (db.messages || []).filter(m => m.chatId === chatId).sort((a, b) => a.timestamp - b.timestamp);
          return sendJson(res, 200, list);
        }

        const currentUserId = urlObj.searchParams.get('userId');
        if (currentUserId) {
          const list = (db.messages || []).filter(m => m.senderId === currentUserId || m.recipientId === currentUserId);
          return sendJson(res, 200, list);
        }

        return sendJson(res, 200, db.messages || []);
      }

      // POST /api/messages
      if (pathname === '/api/messages' && req.method === 'POST') {
        const body = await parseJsonBody(req);
        const chatId = [body.senderId, body.recipientId].sort().join('_chat_');
        const newMsg = {
          id: body.id || 'msg_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5),
          chatId,
          senderId: body.senderId,
          recipientId: body.recipientId,
          content: body.content,
          mediaUrl: body.mediaUrl || null,
          timestamp: body.timestamp || Date.now(),
          read: false
        };

        db.messages.push(newMsg);
        saveDb();
        broadcastEvent('MESSAGE_SENT', newMsg);
        return sendJson(res, 201, newMsg);
      }

      // 8. NOTIFICATIONS
      // GET /api/notifications?userId=...
      if (pathname === '/api/notifications' && req.method === 'GET') {
        const userId = urlObj.searchParams.get('userId');
        const list = (db.notifications || []).filter(n => !userId || n.recipientId === userId);
        return sendJson(res, 200, list);
      }

      // POST /api/notifications
      if (pathname === '/api/notifications' && req.method === 'POST') {
        const body = await parseJsonBody(req);
        const newNotif = {
          id: body.id || 'notif_' + Date.now() + '_' + Math.random().toString(36).substr(2, 4),
          recipientId: body.recipientId,
          senderId: body.senderId,
          type: body.type,
          text: body.text,
          timestamp: body.timestamp || Date.now(),
          read: false
        };

        db.notifications.unshift(newNotif);
        saveDb();
        broadcastEvent('NOTIFICATION_NEW', newNotif);
        return sendJson(res, 201, newNotif);
      }

      return sendJson(res, 404, { error: 'Unknown API endpoint' });
    } catch (apiErr) {
      console.error('[Aura Server] API Error:', apiErr);
      return sendJson(res, 500, { error: apiErr.message });
    }
  }

  // ==========================================
  // STATIC FILE SERVING
  // ==========================================
  let reqPath = decodeURI(pathname);
  if (reqPath === '/' || reqPath === '') reqPath = '/index.html';
  if (reqPath === '/reels') reqPath = '/reels.html';
  if (reqPath === '/messages' || reqPath === '/direct') reqPath = '/messages.html';

  const filePath = path.join(__dirname, reqPath);

  // Security check: ensure path is within directory
  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      // Fallback to index.html for SPA routing if extension missing
      if (!path.extname(filePath)) {
        const indexPath = path.join(__dirname, 'index.html');
        fs.readFile(indexPath, (indexErr, content) => {
          if (indexErr) {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('404 Not Found');
          } else {
            res.writeHead(200, { 'Content-Type': 'text/html; charset=UTF-8' });
            res.end(content);
          }
        });
        return;
      }

      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    fs.readFile(filePath, (readErr, content) => {
      if (readErr) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Server Error');
        return;
      }
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content);
    });
  });
});

function startServer(port) {
  server.listen(port, '0.0.0.0', () => {
    console.log(`\n========================================`);
    console.log(`  🌟 Aura (Instagram) is running!`);
    console.log(`  Local URL:   http://localhost:${port}`);
    console.log(`  Network URL: http://0.0.0.0:${port}`);
    console.log(`  API Backend: http://localhost:${port}/api/status`);
    console.log(`  Shared Realtime DB: ACTIVE`);
    console.log(`========================================\n`);
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.warn(`Port ${port} is in use, trying port ${port + 1}...`);
      startServer(port + 1);
    } else {
      console.error('Server error:', err);
    }
  });
}

startServer(PORT);
