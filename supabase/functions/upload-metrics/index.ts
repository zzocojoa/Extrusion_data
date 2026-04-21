// supabase/functions/upload-metrics/index.ts

// Supabase Edge Functions (Deno)용 v2 클라이언트
import { createClient, type SupabaseClient } from "jsr:@supabase/supabase-js@2";

type Metric = {
  timestamp: string;
  // device_id: string; // Removed
  temperature?: number | null;
  main_pressure?: number | null;
  billet_length?: number | null;
  container_temp_front?: number | null;
  container_temp_rear?: number | null;
  production_counter?: number | null;
  current_speed?: number | null;
  extrusion_end_position?: number | null;
  // New columns
  mold_1?: number | null;
  mold_2?: number | null;
  mold_3?: number | null;
  mold_4?: number | null;
  mold_5?: number | null;
  mold_6?: number | null;
  billet_temp?: number | null;
  at_pre?: number | null;
  at_temp?: number | null;
  die_id?: string | null;
  billet_cycle_id?: number | null;
};

const ALLOWED_KEYS = new Set<keyof Metric>([
  "timestamp",
  // "device_id",
  "temperature",
  "main_pressure",
  "billet_length",
  "container_temp_front",
  "container_temp_rear",
  "production_counter",
  "current_speed",
  "extrusion_end_position",
  "mold_1",
  "mold_2",
  "mold_3",
  "mold_4",
  "mold_5",
  "mold_6",
  "billet_temp",
  "at_pre",
  "at_temp",
  "die_id",
  "billet_cycle_id",
]);

const UPSERT_BATCH_MAX_RECORDS = 1000;
const UPSERT_BATCH_MAX_BYTES = 512 * 1024;
const JSON_ENCODER = new TextEncoder();

function getRecordSizeBytes(record: Metric): number {
  return JSON_ENCODER.encode(JSON.stringify(record)).length + 1;
}

// 업서트 요청을 레코드 수와 대략적인 JSON 크기 기준으로 다시 나눈다.
function splitIntoUpsertBatches(
  records: ReadonlyArray<Metric>,
  maxRecords: number,
  maxBytes: number,
): Metric[][] {
  const batches: Metric[][] = [];
  let currentBatch: Metric[] = [];
  let currentBytes = 0;

  for (const record of records) {
    const recordBytes = getRecordSizeBytes(record);
    const exceedsBatchLimit = currentBatch.length > 0 &&
      (
        currentBatch.length + 1 > maxRecords ||
        currentBytes + recordBytes > maxBytes
      );

    if (exceedsBatchLimit) {
      batches.push(currentBatch);
      currentBatch = [];
      currentBytes = 0;
    }

    currentBatch.push(record);
    currentBytes += recordBytes;
  }

  if (currentBatch.length > 0) {
    batches.push(currentBatch);
  }

  return batches;
}

// 숫자 필드 안전 캐스팅
function toNumberOrNull(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

// 레코드 정제: 허용된 키만 남기고, 타입 맞추기
function cleanRecord(raw: Record<string, unknown>): Metric | null {
  const cleaned: Partial<Metric> = {};

  for (const [key, value] of Object.entries(raw)) {
    if (!ALLOWED_KEYS.has(key as keyof Metric)) continue;

    if (key === "timestamp") {
      if (typeof value !== "string") return null;
      cleaned[key] = value;
    } else if (key === "die_id") {
      // die_id는 문자열 (null 허용)
      if (value === null || value === undefined) {
        cleaned[key] = null;
      } else {
        cleaned[key] = String(value);
      }
    } else {
      // 나머지 키는 모두 숫자형(또는 null)
      (cleaned as any)[key] = toNumberOrNull(value);
    }
  }

  if (typeof cleaned.timestamp !== "string") {
    return null;
  }

  return cleaned as Metric;
}

// 환경변수 헬퍼 (로그만 찍고 undefined 반환)
function getEnv(name: string): string | undefined {
  const value = Deno.env.get(name);
  if (!value) {
    console.warn(`Environment variable ${name} is not set`);
  }
  return value;
}

// Supabase 클라이언트 생성
function createSupabaseClient(req: Request): SupabaseClient {
  const url = getEnv("SUPABASE_URL") ??
    "http://supabase_kong_Extrusion_data:8000"; // 컨테이너 이름 사용

  const anonKey = getEnv("SUPABASE_ANON_KEY");
  const authHeader = req.headers.get("Authorization");

  if (!anonKey) {
    throw new Error("SUPABASE_ANON_KEY is not set");
  }

  return createClient(url, anonKey, {
    global: {
      headers: {
        Authorization: authHeader ?? "",
      },
    },
    auth: {
      persistSession: false,
    },
  });
}

// 공통 JSON 응답 헬퍼
function jsonResponse(
  body: unknown,
  status: number,
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

// 메인 핸들러
Deno.serve(async (req: Request): Promise<Response> => {
  // 1) 메서드 체크
  console.log("Edge Function Filter: Version 2.0 (No Device ID)");
  if (req.method === "GET") {
    // GET: 최신 타임스탬프 조회 (Smart Sync)
    // device_id 파라미터는 무시합니다 (이제 단일 스트림)

    let supabase: SupabaseClient;
    try {
      supabase = createSupabaseClient(req);
    } catch (e) {
      return jsonResponse(
        { success: false, error: "Server config error" },
        500,
      );
    }

    const { data, error } = await supabase
      .from("all_metrics")
      .select("timestamp")
      // .eq("device_id", deviceId) // Removed filtering
      .order("timestamp", { ascending: false })
      .limit(1)
      .single();

    if (error && error.code !== "PGRST116") { // PGRST116: No rows found
      return jsonResponse({ success: false, error: error.message }, 500);
    }

    return jsonResponse(
      {
        success: true,
        latest_timestamp: data?.timestamp || null,
      },
      200,
    );
  }

  if (req.method !== "POST") {
    return jsonResponse(
      {
        success: false,
        error: "Method not allowed. Use POST or GET.",
      },
      405,
    );
  }

  // 2) JSON 파싱
  let body: unknown;
  try {
    body = await req.json();
  } catch (e) {
    console.error("Failed to parse JSON body:", e);
    return jsonResponse(
      {
        success: false,
        error: "Invalid JSON body",
      },
      400,
    );
  }

  // 3) records 배열 추출
  let rawRecords: unknown;
  if (Array.isArray(body)) {
    rawRecords = body;
  } else if (
    body &&
    typeof body === "object" &&
    Array.isArray((body as any).records)
  ) {
    rawRecords = (body as any).records;
  } else {
    return jsonResponse(
      {
        success: false,
        error: 'Request body must be an array or contain a "records" array',
      },
      400,
    );
  }

  if (!Array.isArray(rawRecords) || rawRecords.length === 0) {
    return jsonResponse(
      {
        success: false,
        error: "No records provided",
      },
      400,
    );
  }

  // 4) 레코드 정제 & 필터
  const cleaned: Metric[] = [];
  for (const item of rawRecords) {
    if (!item || typeof item !== "object") continue;
    const rec = cleanRecord(item as Record<string, unknown>);
    if (rec) cleaned.push(rec);
  }

  if (cleaned.length === 0) {
    return jsonResponse(
      {
        success: false,
        error: "No valid records after cleaning",
      },
      400,
    );
  }

  // 5) Supabase 클라이언트 준비
  let supabase: SupabaseClient;
  try {
    supabase = createSupabaseClient(req);
  } catch (e) {
    console.error("Failed to create Supabase client:", e);
    return jsonResponse(
      {
        success: false,
        error: "Server configuration error",
      },
      500,
    );
  }

  // 6) upsert (재분할 후 배치 처리)
  const upsertBatches = splitIntoUpsertBatches(
    cleaned,
    UPSERT_BATCH_MAX_RECORDS,
    UPSERT_BATCH_MAX_BYTES,
  );
  let totalInserted = 0;

  for (const batch of upsertBatches) {
    const { data, error } = await supabase
      .from("all_metrics")
      .upsert(batch, {
        onConflict: "timestamp", // Changed from timestamp,device_id
        ignoreDuplicates: true,
      })
      .select("timestamp");

    if (error) {
      console.error("Supabase upsert error:", error);
      return jsonResponse(
        {
          success: false,
          error: error.message,
        },
        500,
      );
    }

    totalInserted += Array.isArray(data) ? data.length : 0;
  }

  // 7) 성공 응답
  return jsonResponse(
    {
      success: true,
      inserted: totalInserted,
    },
    200,
  );
});
