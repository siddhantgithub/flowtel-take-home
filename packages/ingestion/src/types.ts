export interface RawEvent {
  id: string;
  sessionId?: string;
  userId?: string;
  type?: string;
  name?: string;
  properties?: Record<string, unknown>;
  timestamp?: number | string | unknown;
  session?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface ApiPagination {
  limit: number;
  hasMore: boolean;
  nextCursor: string | null;
  cursorExpiresIn: number | null;
}

export interface ApiMeta {
  total: number;
  returned: number;
  requestId: string;
}

export interface ApiResponse {
  data: RawEvent[];
  pagination: ApiPagination;
  meta: ApiMeta;
}

export interface SessionData {
  id: string;
  userId: string;
  deviceType?: string;
  browser?: string;
  os?: string;
  country?: string;
  city?: string;
  startedAt?: string;
  endedAt?: string;
  duration?: number;
  eventCount: number;
}

export interface SessionsApiResponse {
  data: SessionData[];
  pagination: ApiPagination;
  meta: ApiMeta;
}

export interface Checkpoint {
  cursor: string | null;
  eventsSaved: number;
  updatedAt: Date;
}

export interface IngestionStats {
  totalSaved: number;
  startTime: number;
  lastLogTime: number;
}
